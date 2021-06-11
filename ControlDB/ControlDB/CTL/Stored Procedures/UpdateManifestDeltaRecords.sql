CREATE PROCEDURE [CTL].[UpdateManifestDeltaRecords] (
	@BatchExecutionLogID bigint,
	@TaskExecutionLogID bigint,
	@RecordsDeltaTable bigint)
AS

BEGIN
	UPDATE CTL.ControlManifest
	SET RecordCountDeltaTable = @RecordsDeltaTable
	WHERE BatchExecutionLogID = @BatchExecutionLogID
	AND TaskExecutionLogID = @TaskExecutionLogID
END