CREATE PROCEDURE [CTL].[UpdateManifestAudit] (
	@BatchExecutionLogID bigint,
	@SourceObject varchar(255),
	@ProcessedToTrusted char(1),
	@PipelineRunID varchar(50))
AS

BEGIN
	IF @ProcessedToTrusted = '1'
		UPDATE CTL.ControlManifest SET 
		ProcessedToCleansedZone = 1
		,CleansedZonePipelineRunID = @PipelineRunID
		WHERE ProcessedToCleansedZone IS NULL
		AND BatchExecutionLogID <= @BatchExecutionLogID
		AND SourceObject = @SourceObject
END