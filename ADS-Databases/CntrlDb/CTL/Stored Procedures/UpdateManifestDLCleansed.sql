CREATE PROCEDURE [CTL].[UpdateManifestDLCleansed] (
	@SourceObject varchar(255),
	@BatchExecutionLogID bigint,
	@StartCounter varchar(255),
	@PipelineRunID varchar(50))
AS

BEGIN
	UPDATE CTL.ControlManifest SET 
	[ProcessedToCleansedZone] = 1
	,CleansedZonePipelineRunID = @PipelineRunID
	WHERE [ProcessedToCleansedZone] IS NULL
	AND BatchExecutionLogID = @BatchExecutionLogID
	AND SourceObject = @SourceObject
	AND StartCounter = @StartCounter
END