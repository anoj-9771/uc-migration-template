CREATE PROCEDURE [CTL].[UpdateManifestDLTrusted] (
	@SourceObject varchar(255),
	@BatchExecutionLogID bigint,
	@StartCounter varchar(255),
	@PipelineRunID varchar(50))
AS

BEGIN
	UPDATE CTL.ControlManifest SET 
	[ProcessedToTrustedZone] = 1
	,TrustedZonePipelineRunID = @PipelineRunID
	WHERE [ProcessedToTrustedZone] IS NULL
	AND BatchExecutionLogID = @BatchExecutionLogID
	AND SourceObject = @SourceObject
	AND StartCounter = @StartCounter
END