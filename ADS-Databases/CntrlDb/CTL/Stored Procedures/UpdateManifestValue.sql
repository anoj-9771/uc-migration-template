CREATE PROCEDURE [CTL].[UpdateManifestValue]
	@BatchExecutionLogID bigint,
	@TaskExecutionLogID bigint,
	@SourceFileDateStamp char(14),
	@ManifestValue bigint
AS
	
BEGIN
	UPDATE CTL.ControlManifest_New
	SET ManifestValue = @ManifestValue
	WHERE BatchExecutionLogID = @BatchExecutionLogID
		AND TaskExecutionLogID = @TaskExecutionLogID
		AND SourceFileDateStamp = @SourceFileDateStamp
END