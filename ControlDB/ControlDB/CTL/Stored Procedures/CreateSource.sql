

CREATE PROC [CTL].[CreateSource] 
        @ProjectName		 varchar(100),
		@StartStageName		 varchar(100),
		@EndStageName		 varchar(100),
		@SourceName			 varchar(100),
		@SourceObjectName	 varchar(500),
		@SourceType			 varchar(100),
		@DataLoadMode		 varchar(100),
		@SourceSecretName	 varchar(100),
		@DLRawSecret		 varchar(100),
		@DLStagedSecret		 varchar(100),
		@DBProcessor		 varchar(100),
		@StageDBSecret		 varchar(100),
		@DLRawSubFolder		 varchar(100),
		@DLRawType			 varchar(100),
		@DLStagedMainFolder	 varchar(100),
		@DLStagedSubFolder	 varchar(100),
		@DLStagedType		 varchar(100),
		@DLObjectGrain		 varchar(100),
		@SourceCommand		 varchar(5000),
		@DLRawtoStageCommand varchar(5000),
		@DLStagetoDBCommand  varchar(5000),
		@TargetObjectType	 varchar(100),
		@TargetOverride		 varchar(150),
		@BusinessKeyColumn	 varchar(150),
		@WatermarkColumn	 varchar(150),
		@TrackChanges		 varchar(100),
		@AdditionalProperty	 varchar(max),
		@IsAuditTable		 varchar(100) = '', --Add column to track if the table is a Audit Table
		@SoftDeleteSource	 varchar(100) = '' --Name of the source to be used for Soft Delete
as 

declare @TargetName varchar(1000), @TargetLocation varchar(1000)
SET @TargetName = @SourceObjectName
SET @TargetLocation = @DLRawSubFolder

declare @project int
declare @startStage int, @endStage int
declare @sourceTypeId int, @rawTypeId int, @stageTypeId int, @databricksTypeId int
declare @taskName varchar(650)

declare @error varchar(1000)
declare @counter int
declare @currentStage varchar(50)

--Default option is to load all tables to SQLEDW. If any table does not need to be loaded, then manually run the UPDATE command
DECLARE @LoadToSqlEDW bit = 1

--Update Flags based on value
declare @TrackChangeFlag bit
IF UPPER(@TrackChanges) IN ('YES', 'Y') 
	SET @TrackChangeFlag = 1
ELSE
	SET @TrackChangeFlag = 0

DECLARE @IsAuditTableFlag bit
IF UPPER(@IsAuditTable) IN ('YES', 'Y') 
	SET @IsAuditTableFlag = 1
ELSE
	SET @IsAuditTableFlag = 0

--If Data Load Mode is TRUNCATE-LOAD then TrackChange is false
IF @DataLoadMode = 'TRUNCATE-LOAD'
	SET @TrackChangeFlag = 0

IF @DataLoadMode = 'INCREMENTAL' AND @WatermarkColumn = ''
BEGIN
	RAISERROR('Please specify a WatermarkColumn when the data load mode is INCREMENTAL', 16, 1)
	RETURN
END

IF @DataLoadMode != 'TRUNCATE-LOAD' AND @BusinessKeyColumn = '' AND @StartStageName = 'Source to Staging'
BEGIN
	RAISERROR('Please specify a BusinessKeyColumn column when the data load mode is not TRUNCATE-LOAD', 16, 1) 
	RETURN
END

IF @TrackChangeFlag = 1 AND @BusinessKeyColumn = ''
BEGIN
	RAISERROR('Please specify a BusinessKeyColumn column when TrackChanges is Yes', 16, 1) 
	RETURN
END

--Get project id
set @project = (select ProjectId from [CTL].[ControlProjects] where ProjectName = @ProjectName)

set @error = 'Invalid project name: ' + @ProjectName
if @project is null raiserror(@error,16,1)

--Get start stage id
set @startStage = (select ControlStageId from [CTL].[ControlStages] where StageName = @StartStageName)

set @error = 'Invalid starting stage: ' + @StartStageName
if @startStage is null raiserror(@error,16,1)

--Get end stage id
set @endStage = (select ControlStageId from [CTL].[ControlStages] where StageName = @EndStageName)

set @error = 'Invalid starting stage: ' + @EndStageName
if @endStage is null raiserror(@error,16,1)

--Get source type id
set @sourceTypeId = (select TypeId from [CTL].[ControlTypes] where ControlType = @SourceType)

set @error = 'Invalid source type: ' + @SourceType
if @sourceTypeId is null raiserror(@error,16,1)

--Get raw type id
set @rawTypeId = (select TypeId from [CTL].[ControlTypes] where ControlType = @DLRawType)

set @error = 'Invalid staging zone type: ' + @DLRawType
if @rawTypeId is null raiserror(@error,16,1)

--Get stage type id
set @stageTypeId = (select TypeId from [CTL].[ControlTypes] where ControlType = @DLStagedType)

set @error = 'Invalid stage zone type: ' + @DLStagedType
if @stageTypeId is null raiserror(@error,16,1)


DECLARE @StageFolder varchar(100)
SET @StageFolder =  @DLStagedMainFolder + '/' + @DLStagedSubFolder

--set @deltaFlag = case when @Delta = 'Yes' then 1 else 0 end
set @databricksTypeId = (select typeid from [CTL].[ControlTypes] where ControlType = 'Databricks')

SET @SoftDeleteSource = TRIM(@SoftDeleteSource)
SET @AdditionalProperty = TRIM(@AdditionalProperty)

set @counter = @startStage
While @counter <= @endStage
begin
	
	PRINT 'Starting Stage : ' + CONVERT(VARCHAR, @counter)

	set @currentStage = (select StageName from [CTL].[ControlStages] where ControlStageId = @counter)
	set @taskName = replace(replace(trim(@SourceName + '_' + @SourceObjectName),'[',''),']','')
	set @targetName = replace(replace(trim(@SourceObjectName),'[',''),']','')

	PRINT @currentStage + ' - ' + @taskName + ' - ' + @targetName

	if @currentStage = 'Source to Staging'
	BEGIN
		EXEC [CTL].[CreateTask] 
			@SourceName
			,@SourceObjectName
			,@sourceTypeId
			,@SourceSecretName
			,@DBProcessor
			,@TargetName
			,@TargetLocation
			,@rawTypeId
			,@DLRawSecret
			,@counter
			,@project
			,@sourceTypeId
			,@SourceCommand
			,@DataLoadMode
			,@DLObjectGrain
			,@BusinessKeyColumn
			,@WatermarkColumn
			,@TrackChangeFlag
			,@AdditionalProperty
			,@LoadToSqlEDW
			,@IsAuditTableFlag
			,@SoftDeleteSource

	END

    if @currentStage = 'Staging to Processing'
	BEGIN
		EXEC [CTL].[CreateTask] 
			@SourceName
			,@TargetLocation
			,@rawTypeId
			,@DLRawSecret
			,@DBProcessor
			,@DLRawSubFolder
			,@StageFolder
			,@StageTypeId
			,@DLStagedSecret
			,@counter
			,@project
			,@databricksTypeId
			,@DLRawtoStageCommand
			,@DataLoadMode
			,@DLObjectGrain
			,@BusinessKeyColumn
			,@WatermarkColumn
			,@TrackChangeFlag
			,@AdditionalProperty
			,@LoadToSqlEDW
			,@IsAuditTableFlag
			,@SoftDeleteSource

	END
	

	if @currentStage = 'Processing to Curated'
	BEGIN

		EXEC [CTL].[CreateTask] 
			@SourceName
			,@DLStagedMainFolder
			,@stageTypeId
			,@DLStagedSecret
			,@DBProcessor
			,@DLRawSubFolder 
			,@targetName
			,1
			,@StageDBSecret
			,@counter
			,@project
			,@databricksTypeId
			,@DLStagetoDBCommand
			,0
			,@DLObjectGrain
			,@BusinessKeyColumn
			,@WatermarkColumn
			,@TrackChangeFlag
			,@AdditionalProperty
			,@LoadToSqlEDW
			,@IsAuditTableFlag
			,@SoftDeleteSource
	END	

	if @currentStage IN ('Cubes', 'Synapse Export')
	BEGIN

		SET @TargetLocation = REPLACE(@SourceObjectName, '.', '_')

		EXEC [CTL].[CreateTask] 
			@SourceName
			,@SourceObjectName
			,@SourceTypeId
			,'' --SourceSecretName
			,@DBProcessor
			,@TargetName
			,@TargetLocation
			,@SourceTypeId
			,@SourceSecretName
			,@counter
			,@project
			,@sourceTypeId
			,@SourceCommand
			,@DataLoadMode
			,@DLObjectGrain
			,@BusinessKeyColumn
			,@WatermarkColumn
			,@TrackChangeFlag
			,@AdditionalProperty
			,@LoadToSqlEDW
			,@IsAuditTableFlag
			,@SoftDeleteSource

	END
	set @counter += 1
end