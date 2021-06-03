
--EXEC CTL.sp_CreateControlEntry @ProjectName= 'FEMS', @StartStageName = 'Source to Raw', @EndStageName = 'Trusted to Staging', @ProcessName= 'FEMS_dbo_BookingCancellationReasons', @ProcessType = 'SQL Server', @ProcessGrain = 'Daily', @SourceName = 'FEMS', @SourceObjectName = '[dbo].[BookingCancellationReasons]', @TaskType= 'SQL Server_Full', @SourceType = 'SQL Server', @SourceDataStore = 'FEMS-SQL', @SourceWorker='ADF-SHIR', @SourceLocation = 'dbo.BookingCancellationReasons', @SourceDescription= '[dbo].[BookingCancellationReasons] Table', @DLRawMainFolder= 'raw/FEMS', @DLRawSubFolder = 'dbo_BookingCancellationReasons', @DLRawDescription = 'Raw data from [dbo].[BookingCancellationReasons] table loaded to initial data lake zone', @DLRawDataStore= 'vwdevaueraw', @DLRawType = 'BLOB Storage_csv', @DLRawWorker = 'Standard_DS3_v2_Worker1', @DLStagedMainFolder = 'stage/FEMS', @DLStagedSubFolder = 'dbo_BookingCancellationReasons', @DLStagedDescription = 'Staged data from [dbo].[BookingCancellationReasons] table loaded to secondary data lake zone', @DLStagedType = 'BLOB Storage_parquet', @DLStagedDataStore = 'vwdevauestage', @DLStagedWorker = 'Standard_DS3_v2_Worker1', @StagingDWDataStore = 'vw-dev-dataplatform-aue-sql01_EDW_Stage', @StagingDWType = 'Azure SQL Server',@StagingDWObjectName = 'FEMS.dbo_BookingCancellationReasons', @StagingDWDescription = ' Staged data from the EBS [dbo].[BookingCancellationReasons] table stored into EDW_Stage', @SourcetoFileshareCommand = 'Select * from [dbo].[BookingCancellationReasons]', @DLRawtoStageCommand = '/build/DataProcessing/RawToStage-dp.py', @DLStagetoDWCommand = '/build/DataProcessing/StageToDW_BulkLoad-dp.py',@TargetObjectType= '', @TargetOverride= ''



CREATE        PROC [CTL].[sp_CreateControlEntry] 
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
		@FileShareMainFolder		varchar(50),
		@FileShareSubFolder			varchar(250),
		@FileShareDescription		varchar(1000),
		@FileShareDataStore			varchar(50),
		@FileShareType				varchar(50),
		@DLRawMainFolder			varchar(50),
		@DLRawSubFolder				varchar(250),
		@DLRawDescription			varchar(1000),
		@DLRawDataStore				varchar(50),
		@DLRawType					varchar(50),
		@DLCDCMainFolder			varchar(2000),
		@DLCDCSubFolder				varchar(250),
		@DLCDCDescription	    	varchar(1000),
		@DLCDCType				    varchar(50),
		@DLCDCDataStore				varchar(50),
		@LandingDWDataStore			varchar(50), 
		@LandingDWType				varchar(50), 
		@LandingDWObjectName		varchar(50),  
		@LandingDWDescription		varchar(1000),
		@StagingDWDataStore			varchar(50), 
		@StagingDWType 				varchar(50), 
		@StagingDWObjectName 		varchar(50),  
		@StagingDWDescription 		varchar(1000), 
		@XFormDWDataStore 			varchar(50), 
		@XFormDWType 				varchar(50),  
		@XFormDWObjectName 			varchar(50), 
		@XFormDWDescription 		varchar(1000), 
		@SourcetoFileshareWorker	varchar(50),
		@FilesharetoRawWorker		varchar(50),
		@RawtoLandingWorker			varchar(50),
		@RawtoSTGWorker				varchar(50),
		@RawtoCDCWorker				varchar(50),
		@LandingtoSTGWorker			varchar(50),
		@STGtoXFormWorker			varchar(50),
		@SourcetoFileshareCommand	varchar(max),
		@FilesharetoRawCommand		varchar(max),
		@RawtoLandingCommand		varchar(max),
		@RawtoSTGCommand			varchar(max),
		@RawtoCDCCommand			varchar(max),
		@LandingtoSTGCommand		varchar(max),
		@STGtoXFormCommand			varchar(max),
		@TargetObjectType			varchar(250),
		@TargetOverride				varchar(150)
		
as 
--Declare variables
declare @project int
declare @StageId int
declare @endStage int
declare @processTypeId int, @taskTypeId int, @sourceTypeId int, @rawTypeId int, @stageTypeId int, @databricksTypeId int, @SynapseTypeId int, @sqlTypeId int
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
	set @stageTypeId = (select TypeId from CTL.[Type] where [Name] = @DLCDCType)
	
	set @error = 'Invalid trusted zone type: ' + @DLCDCType
	if @stageTypeId is null raiserror(@error,16,1)
	
	--set @processName = TRIM(@SourceName) + '_' + replace(replace(replace(TRIM(@SourceObjectName),'.','_'),'[',''),']','')
	
	set @prc_counter += 1
end

set @databricksTypeId = (select typeid from CTL.[Type] where [Name] = 'Databricks')
set @SynapseTypeId = (select typeid from CTL.[Type] where [Name] = 'Azure Synapse')
set @sqlTypeId = (select typeid from CTL.[Type] where [Name] = 'SQL Server')
set @stagingTable = @StagingDWObjectName
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
		
		if @stg_current = 'Source->Fileshare' 
			EXEC [CTL].[sp_CreateUpdateTask] @taskProcessId = @processId, @taskStageId = @StageId, @sourceDatasetName = @sourceDSName, @sourceDatasetDescription = @SourceDescription, @sourceDataStore = @SourceDataStore, @sourceDatasetLocation = @sourceLocation, @sourceTypeId = @sourceTypeId, @targetDatasetName = @FileshareSubFolder , @targetDatasetDescription =  @FileshareDescription, @targetDataStore =  @FileshareDataStore, @targetDatasetLocation =  @FileshareMainFolder, @targetTypeId = @rawTypeId, @taskCommand = @SourcetoFileshareCommand, @taskWorker = @SourcetoFileshareWorker, @taskTypeId = @taskTypeId, @taskRank = @stg_counter	

		if @stg_current = 'Fileshare->Raw'
			EXEC [CTL].[sp_CreateUpdateTask] @taskProcessId = @processId, @taskStageId = @StageId, @sourceDatasetName = @FileshareSubFolder, @sourceDatasetDescription = @FileshareDescription, @sourceDataStore = @FileshareDataStore, @sourceDatasetLocation = @FileshareMainFolder, @sourceTypeId = @rawTypeId, @targetDatasetName = @DLRawSubFolder , @targetDatasetDescription = @DLRawDescription, @targetDataStore =  @DLRawDataStore, @targetDatasetLocation = @DLRawMainFolder, @targetTypeId = @stageTypeId, @taskCommand = @FilesharetoRawCommand, @taskWorker = @FilesharetoRawWorker , @taskTypeId = 7, @taskRank = @stg_counter	
	
		if @stg_current = 'Raw->STG'
		  	EXEC [CTL].[sp_CreateUpdateTask] @taskProcessId = @processId, @taskStageId = @StageId, @sourceDatasetName = @DLRawSubFolder, @sourceDatasetDescription = @DLRawDescription, @sourceDataStore = @DLRawDataStore, @sourceDatasetLocation = @DLRawMainFolder, @sourceTypeId = @stageTypeId, @targetDatasetName = @StagingDWObjectName, @targetDatasetDescription = @StagingDWDescription, @targetDataStore =  @StagingDWDataStore, @targetDatasetLocation = @StagingDWObjectName, @targetTypeId = @SynapseTypeId, @taskCommand = @RawtoSTGCommand, @taskWorker = @RawtoSTGWorker, @taskTypeId = 8, @taskRank = @stg_counter	

		if @stg_current = 'Raw->Landing'
		  	EXEC [CTL].[sp_CreateUpdateTask] @taskProcessId = @processId, @taskStageId = @StageId, @sourceDatasetName = @DLRawSubFolder, @sourceDatasetDescription = @DLRawDescription, @sourceDataStore = @DLRawDataStore, @sourceDatasetLocation = @DLRawMainFolder, @sourceTypeId = @stageTypeId, @targetDatasetName = @LandingDWObjectName, @targetDatasetDescription = @LandingDWDescription, @targetDataStore =  @LandingDWDataStore, @targetDatasetLocation = @LandingDWObjectName, @targetTypeId = @SynapseTypeId, @taskCommand = @RawtoLandingCommand, @taskWorker = @RawtoLandingWorker, @taskTypeId = 12, @taskRank = @stg_counter	
	
		if @stg_current = 'Landing->STG'
		  	EXEC [CTL].[sp_CreateUpdateTask] @taskProcessId = @processId, @taskStageId = @StageId, @sourceDatasetName = @LandingDWObjectName, @sourceDatasetDescription = '', @sourceDataStore = @StagingDWDataStore, @sourceDatasetLocation = @LandingDWObjectName, @sourceTypeId = @stageTypeId, @targetDatasetName = @StagingDWObjectName, @targetDatasetDescription = @StagingDWDescription, @targetDataStore =  @StagingDWDataStore, @targetDatasetLocation = @StagingDWObjectName, @targetTypeId = @SynapseTypeId, @taskCommand = @LandingtoSTGCommand, @taskWorker = @LandingtoSTGWorker, @taskTypeId = 33, @taskRank = @stg_counter	
	
		if @stg_current = 'STG->XForm'
			EXEC [CTL].[sp_CreateUpdateTask] @taskProcessId = @processId, @taskStageId = @StageId, @sourceDatasetName = @StagingDWObjectName, @sourceDatasetDescription = @StagingDWDescription, @sourceDataStore = @StagingDWDataStore, @sourceDatasetLocation = @StagingDWObjectName, @sourceTypeId = @SynapseTypeId, @targetDatasetName = @TargetOverride, @targetDatasetDescription = @XFormDWDescription, @targetDataStore =  @XFormDWDataStore, @targetDatasetLocation = @XFormDWObjectName, @targetTypeId = @SynapseTypeId, @taskCommand = @STGtoXFormCommand, @taskWorker = @STGtoXFormWorker, @taskTypeId = 17, @taskRank = @stg_counter

		if @stg_current = 'Raw->CDC'
			EXEC [CTL].[sp_CreateUpdateTask] @taskProcessId = @processId, @taskStageId = @StageId, @sourceDatasetName = @DLRawSubFolder, @sourceDatasetDescription = @DLRawDescription, @sourceDataStore = @DLRawDataStore, @sourceDatasetLocation = @DLRawMainFolder, @sourceTypeId = @rawTypeId, @targetDatasetName = @DLCDCSubFolder , @targetDatasetDescription = @DLCDCDescription, @targetDataStore =  @DLCDCDataStore, @targetDatasetLocation = @DLCDCMainFolder, @targetTypeId = @stageTypeId, @taskCommand = @RawtoCDCCommand, @taskWorker = @RawtoCDCWorker, @taskTypeId = 25, @taskRank = @stg_counter	
	
		if @stg_current = 'XForm->DWDim' and TRIM(@TargetObjectType) = 'DIM'
			EXEC [CTL].[sp_CreateUpdateTask] @taskProcessId = @processId, @taskStageId = @StageId, @sourceDatasetName = @sourceDSName, @sourceDatasetDescription = @SourceDescription, @sourceDataStore = @SourceDataStore, @sourceDatasetLocation = @sourceLocation, @sourceTypeId = @sqlTypeId, @targetDatasetName = @TargetOverride, @targetDatasetDescription = @StagingDWDescription, @targetDataStore =  @StagingDWDataStore, @targetDatasetLocation = @TargetOverride, @targetTypeId = @sqlTypeId, @taskCommand = @STGtoXFormCommand, @taskWorker = @STGtoXFormWorker , @taskTypeId = 15, @taskRank = @stg_counter	

		if @stg_current = 'XForm->DWFact' and TRIM(@TargetObjectType) = 'FACT'
			    EXEC [CTL].[sp_CreateUpdateTask] @taskProcessId = @processId, @taskStageId = @StageId, @sourceDatasetName = @sourceDSName, @sourceDatasetDescription = @SourceDescription, @sourceDataStore = @SourceDataStore, @sourceDatasetLocation = @sourceLocation, @sourceTypeId = @sqlTypeId, @targetDatasetName = @TargetOverride, @targetDatasetDescription = @StagingDWDescription, @targetDataStore =  @StagingDWDataStore, @targetDatasetLocation = @TargetOverride, @targetTypeId = @sqlTypeId, @taskCommand = @STGtoXFormCommand, @taskWorker = @STGtoXFormWorker , @taskTypeId = 16, @taskRank = @stg_counter	
		
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