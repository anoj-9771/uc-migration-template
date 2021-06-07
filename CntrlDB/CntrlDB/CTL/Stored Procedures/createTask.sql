



CREATE Procedure [CTL].[CreateTask] 
	@SourceName Varchar(255) = '',
    @SourceLocation Varchar(255) = '',
	@SourceTypeId BigInt = 2,  
	@SourceServer varchar(255) = '',
	@Processor varchar(255) = '',
	@TargetName Varchar(255) = '',
	@TargetLocation Varchar(255) = '',
	@TargetTypeId BigInt = 4,
	@TargetServer varchar(255)= '',
	@StageId BigInt = 2,
	@ProjectId BigInt = 1,
	@CommandType int = 1,
	@Command varchar(max)= '',
	@DataLoadMode varchar(100) = '',
	@Grain varchar(30) = '',
	@BusinessKeyColumn varchar(100) = '',
	@WatermarkColumn varchar(100) = '',
	@TrackChangeFlag bit = 0,
	@AdditionalProperty	varchar(max) = '', 
	@LoadToSqlEDW BIT = 0,
	@IsAuditTable BIT = 0,
	@SoftDeleteSource varchar(100) = ''

As
BEGIN
	Declare	@SourceId BigInt,
	        @TargetId BigInt,
			@TaskId BigInt

	Begin Try
	Begin Transaction

		DECLARE @UseAuditTable INT
		SET @UseAuditTable = 0 --The default value to UseAuditTable is False for now. If Audit Table is to be used, then set the value to 1 manually

		--If the table is a Audit table, update the values for Audit Table
		IF @IsAuditTable = 1
		BEGIN
			PRINT 'Processing records for Audit Table'
			SET @LoadToSqlEDW = 0 --Do not load Audit table to SQLEDW
			SET @DataLoadMode  = 'APPEND' --No Merge required for Audit table
			SET @TrackChangeFlag = 0 --No track changes required

			IF @StageId = 1
			BEGIN
				SET @Command = 'SELECT ' + @BusinessKeyColumn + ', ' + @WatermarkColumn + ' FROM ' + @SourceLocation --Update SQL to fetch only needed columns
			END

		END

		PRINT 'Adding record to ControlSource'
		Insert Into CTL.ControlSource (SourceName, SourceTypeId, SourceLocation, LoadSource, SourceServer, Processor, BusinessKeyColumn, AdditionalProperty, IsAuditTable, SoftDeleteSource, UseAuditTable) 
		Values(@SourceName, @SourceTypeId, @SourceLocation, 1, @SourceServer, @Processor, @BusinessKeyColumn, @AdditionalProperty, @IsAuditTable, @SoftDeleteSource, @UseAuditTable)
		Select @SourceId = @@IDENTITY

		PRINT 'Adding record to ControlTarget'
		Insert Into CTL.ControlTarget (TargetName, TargetTypeId, TargetLocation, TargetEnabled, TargetServer, Compressed) 
		Values (@SourceName, @TargetTypeId, @TargetLocation, 1, @TargetServer, 1)
		Select @TargetId = @@IDENTITY

		PRINT 'Adding record to ControlTasks'
		Insert Into CTL.ControlTasks (TaskName, SourceId, TargetId, TruncateTarget, TaskEnabled, LoadLatestOnly, 
			ExecuteSourceSQLasStoredProc, ControlStageId, ProjectId, ObjectGrain, DataLoadMode, TrackChanges, LoadToSqlEDW)
		Values (@SourceName, @SourceId, @TargetId, 0, 1, 0, 1, @StageId, @ProjectId, @Grain, @DataLoadMode, @TrackChangeFlag, @LoadToSqlEDW)
		Select @TaskId = @@IDENTITY
		
		PRINT 'Adding record to ControlTaskCommand'
		Insert Into CTL.[ControlTaskCommand] (ControlTaskId, CommandTypeId, Command)
		Values (@TaskId, @CommandType, @Command)

		--Updated by Rahul to add the default entry for Watermark for CDC Sources
		IF @DataLoadMode = 'CDC' AND @StageId = 1
		BEGIN
			PRINT 'Inserting initial watermark for CDC' 
			DECLARE @StartLSN varchar(20)
			SELECT @StartLSN = upper(sys.fn_varbintohexstr(sys.fn_cdc_map_time_to_lsn('START-LSN', '2000-01-01 12:00:00.000')))

			INSERT INTO CTL.ControlWatermark (ControlSourceId, SourceColumn, SourceSQL, Watermarks, SourceName)
			VALUES (@TaskId, '__$start_lsn', '', @StartLSN, @SourceName)
		END

		IF @DataLoadMode IN ('INCREMENTAL', 'APPEND') AND @StageId = 1
		BEGIN
			PRINT 'Inserting initial watermark column'

			INSERT INTO CTL.ControlWatermark (ControlSourceId, SourceColumn, SourceSQL, Watermarks, SourceName)
			VALUES (@SourceId, @WatermarkColumn, @WatermarkColumn, '2000-01-01 00:00:00', @SourceName)
		END

		IF NOT EXISTS(SELECT 1 FROM CTL.ControlProjectSchedule WHERE ControlProjectId = @ProjectId AND ControlStageID = @StageId)
		BEGIN
			DECLARE @ProjectName varchar(100)
			SELECT @ProjectName = ProjectName FROM CTL.ControlProjects WHERE ProjectId = @ProjectId
			--If the Project Schedule does not exist, add a default schedule entry
			INSERT INTO CTL.ControlProjectSchedule (ControlProjectId, ControlStageID, TriggerName) VALUES (@ProjectId, @StageId, @ProjectName)
		END


	Commit
	End Try
	Begin Catch

		Rollback
		Declare @err_num  int = @@ERROR
		Declare @err_desc varchar(500) = ERROR_MESSAGE()
		raiserror(@err_desc,@err_num,1)

	End Catch

END