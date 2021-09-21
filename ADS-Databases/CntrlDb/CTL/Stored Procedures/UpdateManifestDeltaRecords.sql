CREATE PROCEDURE [CTL].[UpdateManifestDeltaRecords] (
	@BatchExecutionLogID bigint,
	@TaskExecutionLogID bigint,
	@SourceFileDateStamp char(14),
	@RecordsDeltaTable bigint)
AS

BEGIN
	UPDATE CTL.ControlManifest
	SET RecordCountDeltaTable = @RecordsDeltaTable
	WHERE BatchExecutionLogID = @BatchExecutionLogID
		AND TaskExecutionLogID = @TaskExecutionLogID
		AND SourceFileDateStamp = @SourceFileDateStamp
END