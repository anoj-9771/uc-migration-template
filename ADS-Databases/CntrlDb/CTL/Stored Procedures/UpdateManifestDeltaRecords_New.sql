CREATE PROCEDURE [CTL].[UpdateManifestDeltaRecords_New] (
	@BatchExecutionLogID bigint,
	@TaskExecutionLogID bigint,
	@SourceFileDateStamp char(14),
	@RecordsDeltaTable bigint)
AS

BEGIN
	UPDATE CTL.ControlManifest_New
	SET RecordCountDeltaTable = @RecordsDeltaTable
	WHERE BatchExecutionLogID = @BatchExecutionLogID
		AND TaskExecutionLogID = @TaskExecutionLogID
		AND SourceFileDateStamp = @SourceFileDateStamp
END