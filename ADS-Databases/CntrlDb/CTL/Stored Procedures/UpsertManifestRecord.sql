CREATE PROCEDURE [CTL].[UpsertManifestRecord]
	@BatchExecutionLogID bigint,
	@TaskExecutionLogID bigint,
	@SourceObject varchar(1000),
	@Container varchar(1000),
	@DeltaColumn varchar(100),
	@StartCounter varchar(500),
	@EndCounter varchar(500),
	@Records bigint,
	@RecordsDeltaTable bigint = 0,
	@FolderName varchar(1000),
	@FileName varchar(1000),
	@PipelineRunID varchar(50) = '',
	@SourceFileName varchar(1000),
	@SourceFileDateStamp char(14),
	@IsManifest bit
AS
BEGIN
   IF @IsManifest = 1
   BEGIN
    SET @SourceFileName = replace(@SourceFileName, 'MANIFEST_', '')
	SET @FileName = replace(@SourceFileName, 'MANIFEST_', '')
   END

   IF (
	SELECT COUNT(*) 
	FROM CTL.ControlManifest
	WHERE BatchExecutionLogID = @BatchExecutionLogID 
		AND TaskExecutionLogID  = @TaskExecutionLogID
		AND SourceFileDateStamp = @SourceFileDateStamp
	) = 0 
	BEGIN
		EXEC CTL.CreateManifestRecord 
			@BatchExecutionLogID = @BatchExecutionLogID,
			@TaskExecutionLogID = @TaskExecutionLogID,
			@SourceObject = @SourceObject,
			@Container = @Container,
			@DeltaColumn = @DeltaColumn,
			@StartCounter = @StartCounter,
			@EndCounter = @EndCounter,
			@Records = @Records,
			@RecordsDeltaTable = @RecordsDeltaTable,
			@FolderName = @FolderName,
			@FileName = @FileName,
			@PipelineRunID = @PipelineRunID,
			@SourceFileName = @SourceFileName,
			@SourceFileDateStamp = @SourceFileDateStamp
	END

END

