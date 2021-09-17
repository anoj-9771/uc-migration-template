
CREATE PROCEDURE [CTL].[CreateManifestRecord] (
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
	@SourceFileDateStamp char(14)
)
AS 

SET @DeltaColumn = ISNULL(@DeltaColumn, '')

DECLARE @Start varchar(500)
IF ISDATE(@StartCounter) = 1
	SET @Start = FORMAT(TRY_CONVERT(DATETIME, @StartCounter), 'yyyy-MM-ddTHH:mm:ss')
ELSE
	SET @Start = ISNULL(@StartCounter, '')

DECLARE @End varchar(500)
IF ISDATE(@EndCounter) = 1
	SET @End = FORMAT(TRY_CONVERT(DATETIME, @EndCounter), 'yyyy-MM-ddTHH:mm:ss')
ELSE
	SET @End = @EndCounter


INSERT INTO CTL.ControlManifest(
	[BatchExecutionLogID]
	,[TaskExecutionLogID]
	,[SourceObject]
	,[Container]
	,[DeltaColumn]
	,[StartCounter]
	,[EndCounter]
	,[RecordCountLoaded]
	,[RecordCountDeltaTable]
	,[FolderName]
	,[FileName]
	,[RawZonePipelineRunID]
	,[SourceFileName]
	,[SourceFileDateStamp]
	)
VALUES (
	@BatchExecutionLogID
	,@TaskExecutionLogID
	,@SourceObject
	,@Container
	,@DeltaColumn
	,@Start
	,@End
	,@Records
	,NULL
	,@FolderName
	,@FileName
	,@PipelineRunID
	,@SourceFileName
	,@SourceFileDateStamp
	)