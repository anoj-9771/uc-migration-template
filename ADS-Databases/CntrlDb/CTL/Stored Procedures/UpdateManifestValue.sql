CREATE PROCEDURE [CTL].[UpdateManifestValue]
	@BatchExecutionLogID bigint,
	@TaskExecutionLogID bigint,
	@SourceFileName varchar(1000),
	@ManifestValue bigint
AS
	
BEGIN
	UPDATE CTL.ControlManifest_New
	SET ManifestValue = @ManifestValue
	WHERE BatchExecutionLogID = @BatchExecutionLogID
		AND TaskExecutionLogID = @TaskExecutionLogID
		AND SourceFileName = @SourceFileName
END