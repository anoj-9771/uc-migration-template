CREATE PROCEDURE [CTL].[UpdateManifestDeltaRecords_New] (
	@BatchExecutionLogID bigint,
	@TaskExecutionLogID bigint,
	@SourceFileName varchar(1000),
	@RecordsDeltaTable bigint)
AS

BEGIN
	UPDATE CTL.ControlManifest_New
	SET RecordCountDeltaTable = @RecordsDeltaTable
	WHERE BatchExecutionLogID = @BatchExecutionLogID
		AND TaskExecutionLogID = @TaskExecutionLogID
		AND SourceFileName = @SourceFileName
END