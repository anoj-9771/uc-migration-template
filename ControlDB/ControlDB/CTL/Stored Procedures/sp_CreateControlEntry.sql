CREATE PROC [CTL].[sp_CreateControlEntry] 
        @ProjectName				varchar(50),
		@StageSequence				varchar(5000),
		@ProcessName				varchar(250),
		@ProcessType				varchar(50),
		@ProcessGrain				varchar(50),
		@SourceName					varchar(100),
		@SourceObjectName			varchar(250),
		@TaskType					varchar(50),
		@SourceType					varchar(50),
		@SourceDataStore			varchar(50),
		@SourceLocation				varchar(1000),
		@SourceDescription			varchar(1000),
		@DLRawMainFolder			varchar(50) = null,
		@DLRawSubFolder				varchar(250) = null,
		@DLRawDescription			varchar(1000) = null,
		@DLRawDataStore				varchar(50) = null,
		@DLRawType					varchar(50) = null,
		@DLStageMainFolder			varchar(2000) = null,
		@DLStageSubFolder			varchar(250) = null,
		@DLStageDescription	    	varchar(1000) = null,
		@DLStageType				varchar(50) = null,
		@DLStageDataStore			varchar(50) = null,
		@DLCuratedMainFolder		varchar(2000) = null,
		@DLCuratedSubFolder			varchar(250) = null,
		@DLCuratedDescription	    varchar(1000) = null,
		@DLCuratedType				varchar(50) = null,
		@DLCuratedDataStore			varchar(50) = null,
		@StgEDWDataStore			varchar(50) = null, 
		@StgEDWType 				varchar(50) = null, 
		@StgEDWObjectName 			varchar(50) = null,  
		@StgEDWDescription 			varchar(1000) = null, 
		@XFormEDWDataStore 			varchar(50) = null, 
		@XFormEDWType 				varchar(50) = null,  
		@XFormEDWObjectName 		varchar(50) = null, 
		@XFormEDWDescription 		varchar(1000) =  null, 
		@SourcetoDLRawWorker		varchar(50) = null,
		@DLRawtoDLStageWorker		varchar(50) = null,
		@DLStagetoCuratedWorker		varchar(50) = null,
		@DLStagetoEDWWorker			varchar(50) = null,
		@DLCuratedtoEDWWorker		varchar(50) = null,
		@XFormWorker				varchar(50) = null,
		@SourcetoDLRawCommand		varchar(max) = null,
		@DLRawtoDLStageCommand		varchar(max) = null,
		@DLStagetoCuratedCommand	varchar(max) = null,
		@DLStagetoStgEDWCommand		varchar(max) = null,
		@DLCuratedtoStgEDWCommand	varchar(max) = null,
		@XFormCommand				varchar(max) = null,
		@TargetObjectType			varchar(250) = null,
		@TargetOverride				varchar(150) = null
as 



--Declare variables
declare @project int
declare @StageId int
declare @endStage int
declare @processTypeId int, @taskTypeId int, @sourceTypeId int, @rawTypeId int, @stageTypeId int, @databricksTypeId int, @EDWTypeId int, @sqlTypeId int
declare @deltaFlag int

declare @processId int
declare @taskName varchar(650)
declare @targetName varchar(650)
declare @stagingTable varchar(650)
declare @xFormTable varchar(650)
declare @BI_ReloadTable varchar(650)

declare @error varchar(1000)
declare @counter int
declare @prc_counter int
declare @prc_currentName varchar(500)
declare @maxProcess int
declare @prc_current varchar(1000)  
declare @prc_previous varchar(1000) 
declare @stg_counter int
declare @maxStage int
declare @stg_current varchar(1000) 
declare @stg_previous varchar(1000)
declare @prc_previousId int
declare @prc_previoustName varchar(500)
declare @sourceDSName varchar(250)

-- Declare tables
declare @processes table ([Key] int, [Value] varchar(1000))
declare @stages table ([Key] int, [Value] varchar(100))

-- Insert stage sequences into processes from process groups
insert into @processes select * from ctl.udf_string_split(@StageSequence, ',')
 
-- Get max Process in sequence
set @maxProcess = (select count(*) from @processes)

-- Get project id
set @project = (select ProjectId from CTL.Project where [Name] = @ProjectName)
set @error = 'Invalid project name: ' + @ProjectName
if @project is null raiserror(@error,16,1)

						/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
						 * Check variable values and raise errors if they do not exist *
						 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

-- Check processes
set @prc_counter = 1
while @prc_counter <= @maxProcess
begin
	set @prc_current = (select [Value] from @processes where [Key] = @prc_counter)
	set @prc_previous = (select [Value] from @processes where [Key] = @prc_counter - 1)
	set @stg_counter = 0

	delete from @stages
	insert into @stages select * from ctl.udf_string_split(@prc_current, ':')
	set @maxStage = (select max([Key]) from @stages)

	-- Check stages
	set @stg_counter = 1
	while @stg_counter <= @maxStage
	begin
		set @stg_current = (select [Value] from @stages where [Key] = @stg_counter)
		set @stg_previous = (select [Value] from @stages where [Key] = @stg_counter - 1)
		-- Check Stage
		--set @StageName = (select [Value] from @stages where [Key] = @stg_counter)
		set @StageId = (select StageId from [CTL].Stage where [Name] = @stg_current)
		set @error = 'Invalid stage: ' + @stg_current
		if @StageId is null raiserror(@error,16,1)
		set @stg_counter += 1
	end
	
	--Get process type id
	set @processTypeId = (select TypeId from CTL.[Type] where [Name] = @ProcessType)
	
	set @error = 'Invalid process type: ' + @ProcessType
	if @processTypeId is null raiserror(@error,16,1)
	
	--Get task type id
	set @taskTypeId = (select TypeId from CTL.[Type] where [Name] = @TaskType)
	
	set @error = 'Invalid task type: ' + @TaskType
	if @taskTypeId is null raiserror(@error,16,1)
	
	--Get source type id
	set @sourceTypeId = (select TypeId from CTL.[Type] where [Name] = @SourceType)
	
	set @error = 'Invalid source type: ' + @SourceType
	if @sourceTypeId is null raiserror(@error,16,1)
	
	--Get raw type id
	set @rawTypeId = (select TypeId from CTL.[Type] where [Name] = @DLRawType)
	
	set @error = 'Invalid raw zone type: ' + @DLRawType
	if @rawTypeId is null raiserror(@error,16,1)
	
	--Get stage type id
	set @stageTypeId = (select TypeId from CTL.[Type] where [Name] = @DLStageType)
	
	set @error = 'Invalid trusted zone type: ' + @DLStageType
	if @stageTypeId is null raiserror(@error,16,1)
	
	--set @processName = TRIM(@SourceName) + '_' + replace(replace(replace(TRIM(@SourceObjectName),'.','_'),'[',''),']','')
	
	set @prc_counter += 1
end

set @databricksTypeId = (select typeid from CTL.[Type] where [Name] = 'Databricks')
set @EDWTypeId = (select typeid from CTL.[Type] where [Name] = 'Azure SQL Server')
set @sqlTypeId = (select typeid from CTL.[Type] where [Name] = 'SQL Server')
set @stagingTable = @StgEDWObjectName
set @xFormTable = @SourceName + '.' + replace(@SourceObjectName,'vw_','')

if @TargetOverride = ''
set @BI_ReloadTable = @SourceName + '.' + replace(@SourceObjectName,'vw_','')
else
set @BI_ReloadTable = @TargetOverride

--set @sourceLocation = Replace(Replace(Replace(@SourceObjectName,' ','_'),'[',''),']','')



set @prc_counter = 1
while @prc_counter <= @maxProcess
begin
	set @prc_current = (select [Value]  from @processes where [Key] = @prc_counter)
	set @prc_previous = (select [Value] from @processes where [Key] = @prc_counter - 1)
	set @prc_currentName = @ProcessName + '_' + right('00' + cast(@prc_counter as varchar(3)), 3)
	set @prc_previoustName = @ProcessName + '_' + right('00' + cast(@prc_counter - 1 as varchar(3)), 3)
	set @prc_previousId = (select ProcessId from ctl.Process where Name = @prc_previoustName)

	print 'Previous process name = ' + @ProcessName + '_' + cast(@prc_counter - 1 as varchar(5))
	print 'Current process name = ' + @ProcessName + '_' + cast(@prc_counter as varchar(5))
	print 'Current stage sequence = ' + @prc_current
	print 'Prevous stage sequence = ' + @prc_previous

	delete from @stages
	
	if @prc_previous is null
	begin
		insert into @stages 
		select * from ctl.udf_string_split(@prc_current, ':') 
	end 
	else
	begin
		insert into @stages 
		select row_number()over(order by (select 1)), [Value] from ctl.udf_string_split(@prc_current, ':') 
		where @prc_previous not like '%:' + @stg_current + ':%'  and [Key] <> 1
		order by [Key]
	end

	set @maxStage = (select count(*) from @stages)
	--Create or update the process
	exec CTL.sp_CreateUpdateProcess @prc_currentName,'',@project,1,1,@ProcessGrain,@processTypeId
	set @processId = (select ProcessId from CTL.Process where [name]  = @prc_currentName and ProjectId = @project)
	
	--Get Stages
	
	set @stg_counter = 1
	while @stg_counter <= @maxStage
	begin
		set @stg_current = (select [Value] from @stages where [Key] = @stg_counter)
		set @stg_previous = (select [Value] from @stages where [Key] = @stg_counter - 1)
		set @StageId = (select StageId from [CTL].Stage where [Name] = @stg_current)
		set @sourceDSName = @SourceName + '_' + @SourceObjectName
		--select * from @stages
		--Insert Stage statements
		--Insert Process Dependency
		print 'Current StageId = ' +  cast(@StageId as varchar(5))
		print 'Current stage = ' + @stg_current
		print 'Previous stage = ' + @stg_previous 
		print case when @prc_previous like '%:' + @stg_current + ':%'  and @stg_counter = 1 
			then  @processName + '_' + cast(@prc_counter as varchar(5)) + 
				' is dependent on ' + 
				@processName + '_' + cast(@prc_counter - 1 as varchar(5)) end
		
		if @stg_current = 'Source->Raw' 
			EXEC [CTL].[sp_CreateUpdateTask] @taskProcessId = @processId, @taskStageId = @StageId, @sourceDatasetName = @sourceDSName, @sourceDatasetDescription = @SourceDescription, @sourceDataStore = @SourceDataStore, @sourceDatasetLocation = @sourceLocation, @sourceTypeId = @sourceTypeId, @targetDatasetName = @DLRawSubFolder , @targetDatasetDescription =  @DLRawDescription, @targetDataStore =  @DLRawDataStore, @targetDatasetLocation =  @DLRawMainFolder, @targetTypeId = @rawTypeId, @taskCommand = @SourcetoDLRawCommand, @taskWorker = @SourcetoDLRawWorker, @taskTypeId = @taskTypeId, @taskRank = @stg_counter	

		if @stg_current = 'Raw->Stage'
		  	EXEC [CTL].[sp_CreateUpdateTask] @taskProcessId = @processId, @taskStageId = @StageId, @sourceDatasetName = @DLRawSubFolder,		@sourceDatasetDescription = @DLRawDescription,		@sourceDataStore = @DLRawDataStore,		@sourceDatasetLocation = @DLRawMainFolder, @sourceTypeId = @rawTypeId, @targetDatasetName = @DLStageSubFolder, @targetDatasetDescription = @DLStageDescription, @targetDataStore =  @DLStageDataStore, @targetDatasetLocation = @DLStageMainFolder, @targetTypeId = @stageTypeId, @taskCommand = @DLRawtoDLStageCommand, @taskWorker = @DLRawtoDLStageWorker, @taskTypeId = 9, @taskRank = @stg_counter	

		if @stg_current = 'Stage->EDW'
			EXEC [CTL].[sp_CreateUpdateTask] @taskProcessId = @processId,  @taskStageId = @StageId,  @sourceDatasetName = @DLStageSubFolder,	@sourceDatasetDescription = @DLStageDescription,	@sourceDataStore = @DLStageDataStore,	@sourceDatasetLocation = @DLStageMainFolder,  @sourceTypeId = @stageTypeId,  @targetDatasetName = @TargetOverride,  @targetDatasetDescription = @StgEDWDescription,  @targetDataStore = @StgEDWDataStore,  @targetDatasetLocation = @StgEDWObjectName,  @targetTypeId = @EDWTypeId,  @taskCommand = @DLStagetoStgEDWCommand,  @taskWorker = @DLStagetoEDWWorker,  @taskTypeId = 9,  @taskRank = @stg_counter

		if @stg_current = 'EDW->XForm'
			EXEC [CTL].[sp_CreateUpdateTask] @taskProcessId = @processId, @taskStageId = @StageId, @sourceDatasetName = @StgEDWObjectName, @sourceDatasetDescription = @StgEDWDescription, @sourceDataStore = @StgEDWDataStore, @sourceDatasetLocation = @StgEDWObjectName, @sourceTypeId = @EDWTypeId, @targetDatasetName = @TargetOverride, @targetDatasetDescription = @XFormEDWDescription, @targetDataStore =  @XFormEDWDataStore, @targetDatasetLocation = @XFormEDWObjectName, @targetTypeId = @EDWTypeId, @taskCommand = @XFormCommand, @taskWorker = @XFormWorker, @taskTypeId = 20, @taskRank = @stg_counter
	
		if @stg_current = 'XForm->DWDim'
			EXEC [CTL].[sp_CreateUpdateTask] @taskProcessId = @processId, @taskStageId = @StageId, @sourceDatasetName = @sourceDSName, @sourceDatasetDescription = @SourceDescription, @sourceDataStore = @SourceDataStore, @sourceDatasetLocation = @sourceLocation, @sourceTypeId = @EDWTypeId, @targetDatasetName = @TargetOverride, @targetDatasetDescription = @XFormEDWDescription, @targetDataStore =  @XFormEDWDataStore, @targetDatasetLocation = @XFormEDWObjectName, @targetTypeId = @sqlTypeId, @taskCommand = @XFormCommand, @taskWorker = @XFormWorker , @taskTypeId = 18, @taskRank = @stg_counter	

		if @stg_current = 'XForm->DWFact'
			    EXEC [CTL].[sp_CreateUpdateTask] @taskProcessId = @processId, @taskStageId = @StageId, @sourceDatasetName = @sourceDSName, @sourceDatasetDescription = @SourceDescription, @sourceDataStore = @SourceDataStore, @sourceDatasetLocation = @sourceLocation, @sourceTypeId = @EDWTypeId, @targetDatasetName = @TargetOverride, @targetDatasetDescription = @StgEDWDescription, @targetDataStore =  @StgEDWDataStore, @targetDatasetLocation = @TargetOverride, @targetTypeId = @sqlTypeId, @taskCommand = @XFormCommand, @taskWorker = @XFormWorker , @taskTypeId = 19, @taskRank = @stg_counter	
		
		if @prc_previous like  '%' + (select [Value] from ctl.udf_string_split(@prc_current, ':') where [Key] = 1) + '%'
		begin
			update ctl.Process set ProcessDependency = 
				case when ProcessDependency = '' or ProcessDependency is null then cast(@prc_previousId as varchar) 
					when  cast(@prc_previousId as varchar)  not in (select [Value] from [CTL].[udf_string_split](ProcessDependency,','))
					then ProcessDependency + ',' + cast(@prc_previousId as varchar) 
					else ProcessDependency end where ProcessId = @processId
			--print 'Previous process ID = ' + cast(@prc_previousId  as varchar)
		end

		set @stg_counter += 1
	end
	set @prc_counter += 1
end