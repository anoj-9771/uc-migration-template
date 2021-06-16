CREATE PROC [CTL].[sp_CreateUpdateTask]
@taskProcessId				int,
@taskStageId				int,
@sourceDatasetName			varchar(250),
@sourceDatasetDescription	varchar(1000), 
@sourceDataStore			varchar(250), 
@sourceDatasetLocation		varchar(250), 
@sourceTypeId				int,
@targetDatasetName			varchar(250),
@targetDatasetDescription	varchar(1000), 
@targetDataStore			varchar(250), 
@targetDatasetLocation		varchar(250), 
@targetTypeId				int,
@taskCommand				varchar(max),
@taskWorker					varchar(100),
@taskTypeId					int,
@taskRank					int

AS

Declare @sourceId int, @targetId int
Declare @sourceDataStoreId int, @targetDataStoreId int
Declare @taskWorkerId int
Declare @taskId int
Declare @error varchar(1000)
declare @json varchar(max)
declare @colCount int
declare @wm_table table (
	RowRank int, 
	DataSetId bigint,
	[TypeID] bigint,
	[Column] varchar(100),
	Operator char(1),
	DataTypeId bigint,
	[Value] varchar(100),
	[BatchGrain] varchar(20)
)



--Get Source Data Store
set @sourceDataStoreId = (select DataStoreId from CTL.[DataStore] where [Name] = TRIM(@sourceDataStore))

set @error = 'Invalid source data store: ' + @sourceDataStore
if @sourceDataStoreId is null raiserror(@error,16,1)

--Get Target Data Store
set @targetDataStoreId = (select DataStoreId from CTL.[DataStore] where [Name] = TRIM(@targetDataStore))

set @error = 'Invalid target data store: ' + @targetDataStore
if @targetDataStoreId is null raiserror(@error,16,1)

--Get Task Worker Id
set @taskWorkerId = (select WorkerId from CTL.[Worker] where [Name] = TRIM(@taskWorker))

set @error = 'Invalid task worker: ' + @taskWorker
if @taskWorkerId is null raiserror(@error,16,1)

Begin Try
Begin Transaction

	--INSERT OR UPDATE SOURCE DATASET
	set @sourceId = (select DataSetId from CTL.DataSet where [Name] = TRIM(@sourceDatasetName) and [DataStoreId] = @sourceDataStoreId)
	
	If @sourceId is null 
	Begin
		Insert Into CTL.DataSet ([Name], [Description], DataStoreId, [Location],[TypeId])
		Values (TRIM(@sourceDatasetName),TRIM(@sourceDatasetDescription),@sourceDataStoreId,TRIM(@sourceDatasetLocation),@sourceTypeId)
		
		set @sourceId = SCOPE_IDENTITY()
	End
	Else
	Begin
		Update CTL.DataSet
		SET Description = TRIM(@sourceDatasetDescription),
		    Location = TRIM(@sourceDatasetLocation)
		Where DataSetId = @sourceId	
	
	END
	
	--INSERT OR UPDATE TARGET DATASET
	set @targetId = (select DataSetId from CTL.DataSet where [Name] = TRIM(@targetDatasetName) and [DataStoreId] = @targetDataStoreId)
	
	If @targetId is null 
	Begin
		Insert Into CTL.DataSet ([Name], [Description], DataStoreId, [Location],[TypeId])
		Values (TRIM(@targetDatasetName),TRIM(@targetDatasetDescription),@targetDataStoreId,TRIM(@targetDatasetLocation),@targetTypeId)
		
		set @targetId = SCOPE_IDENTITY()
	End
	Else
	Begin
		Update CTL.DataSet
		SET Description = TRIM(@targetDatasetDescription),
		    Location = TRIM(@targetDatasetLocation)
		Where DataSetId = @targetId	
	
	END
		
	--INSERT OR UPDATE TASK
	
	set @taskId = (Select TaskId from CTL.Task where ProcessId = @taskProcessId and StageId = @taskStageId)
	
	If @taskId is Null
	Begin
		Insert Into CTL.Task (ProcessId,SourceId,TargetId,StageId,WorkerId,Query, TaskTypeId, [Rank])
		Values
		(@taskProcessId,@sourceId,@targetId,@taskStageId,@taskWorkerId,TRIM(@taskCommand),@taskTypeId,@taskRank)
	End
	Else
	Begin
		Update CTL.Task
		SET SourceId =  @sourceId,
		    TargetId =  @targetId,
			WorkerId = @taskWorkerId,
			Query = case when @taskCommand like '%@@%' then substring(@taskCommand,0, charindex('@@',@taskCommand)) else @taskCommand end,
			TaskTypeId = @taskTypeId,
			[Rank] = @taskRank
		Where ProcessId = @taskProcessId
		And StageId = @taskStageId

	End
	-- INSERT OR UPDATE WATERMARK
	
	-- Delete unreferenced watermarks
	if @taskCommand not like '%@@%'
	begin
		delete from  ctl.Watermark
		where DatasetId = @sourceId
	end

	-- Insert Reference watermarks
	if @taskCommand like '%@@%'
	begin
		select @json = case when @taskCommand like '%@@%' then right(@taskCommand,charindex('@@', reverse(@taskCommand)) - 1) else @taskCommand end
		
		-- Convert delta criteria into temp table
		insert into @wm_table (RowRank, DataSetId, TypeID, [Column], Operator, DataTypeId, [Value], BatchGrain)
		select row_number()over(order by [Column]) RowRank, @sourceId, 
			tp.TypeId, a.[Column], a.Operator, dt_tp.TypeId, a.Value, isnull(a.BatchGrain,'Full') BatchGrain
		from ( select
				json_value(@json, '$.type') [Type],
				json_value(c.value, '$.name' ) as [Column],
				json_value(c.value, '$.operator' ) as Operator,
				json_value(c.value, '$.dataType' ) as DataType,
				json_value(c.value, '$.value' ) as [Value],
				json_value(c.value, '$.batchGrain' ) as [BatchGrain]
		from  openjson(@json, '$.columns')  as c) as a
		join ctl.Type tp on a.[Type] = tp.Name
		join ctl.Type dt_tp on a.DataType = dt_tp.Name
		--select @json = case when @taskCommand like '%@@%' then right(@taskCommand,charindex('@@', reverse(@taskCommand)) - 1) else @taskCommand end
		
		---- Convert delta criteria into temp table
		--insert into @wm_table (RowRank, DataSetId, [Column], Operator, TypeId, [Value])
		--select row_number()over(order by [Column]) RowRank, @sourceId DataSetId, [Column], Operator, tp.TypeId, [Value]
		--from openjson(@json)
		--with(
		--	[Column] varchar(100) '$.column',
		--	Operator char(1) '$.operator',
		--	DataType varchar(100) '$.dataType',
		--	[Value] varchar(100) '$.value'
		--) js
		--join ctl.Type tp on js.DataType = tp.Name
		
		begin
			-- Insert new watermarks
			insert into Watermark (DatasetId, [Column], Operator, TypeId, [Value], [BatchGrain], [DataTypeId])
			select DataSetId, [Column], Operator, TypeId, [Value], [BatchGrain], [DataTypeId]
			from @wm_table wmt
			where concat(wmt.DatasetId, wmt.[Column]) not in (select concat(DatasetId, [Column]) from ctl.Watermark) 

			--Update exisitng watermarks
			update wm	
			set wm.DatasetId = wmt.DataSetId,
				wm.[Column] = wmt.[Column],
				wm.Operator = wmt.Operator,
				wm.TypeId = wmt.TypeId,
				wm.[Value] = wmt.[Value],
				wm.[BatchGrain] = wmt.[BatchGrain],
				wm.[DataTypeId] = wmt.[DataTypeId]
			from ctl.Watermark wm
			join @wm_table wmt on wm.DatasetId = wmt.DataSetId
			where wm.DatasetId = wmt.DataSetId and wm.[Column] = wmt.[Column]

			-- Delete unreferenced watermarks
			delete from  ctl.Watermark
			where concat(DatasetId, [Column]) not in (select concat(DatasetId, [Column]) from @wm_table) 
			and DatasetId = @sourceId
		end

	end


	Commit
End Try
Begin Catch

		Rollback
		Declare @err_num  int = @@ERROR
		Declare @err_desc varchar(500) = ERROR_MESSAGE()
		raiserror(@err_desc,@err_num,1)

End Catch