
--EXEC [CTL].[sp_CreateUpdateTask] @taskProcessId = ,@taskStageId = ,@sourceDatasetName	=  ,@sourceDatasetDescription =  ,@sourceDataStore = ,@sourceDatasetLocation = ,@targetDatasetName =  ,@targetDatasetDescription =  ,@targetDataStore =  ,@targetDatasetLocation =  ,@taskCommand =  ,@taskWorker =  ,@taskTypeId =  ,@taskRank	=				


CREATE     PROC [CTL].[sp_CreateUpdateTask]
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
			Query = @taskCommand,
			TaskTypeId = @taskTypeId,
			[Rank] = @taskRank
		Where ProcessId = @taskProcessId
		And StageId = @taskStageId

	End


	Commit
End Try
Begin Catch

		Rollback
		Declare @err_num  int = @@ERROR
		Declare @err_desc varchar(500) = ERROR_MESSAGE()
		raiserror(@err_desc,@err_num,1)

End Catch