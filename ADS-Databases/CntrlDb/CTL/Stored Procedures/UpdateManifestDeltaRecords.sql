CREATE PROCEDURE [CTL].[UpdateManifestDeltaRecords] (
	@BatchExecutionLogID bigint,
	@TaskExecutionLogID bigint,
	@SourceFileDateStamp char(14),
	@RecordsDeltaTable bigint,
	@RecordsTargetTable bigint
)
AS

BEGIN
	UPDATE CTL.ControlManifest
	SET RecordCountDeltaTable = @RecordsDeltaTable,
		RecordCountTargetTable = @RecordsTargetTable
	WHERE BatchExecutionLogID = @BatchExecutionLogID
		AND TaskExecutionLogID = @TaskExecutionLogID
		AND SourceFileDateStamp = @SourceFileDateStamp
END