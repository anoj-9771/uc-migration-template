CREATE PROCEDURE [CTL].[UpdateManifestValue]
	@BatchExecutionLogID bigint,
	@TaskExecutionLogID bigint,
	@SourceFileDateStamp char(14),
	@M_DeltaRecordCount bigint,
	@M_TotalNoRows bigint
AS
	
BEGIN
	UPDATE CTL.ControlManifest
	SET M_DeltaRecordCount = @M_DeltaRecordCount,
		M_TotalNoRows = @M_TotalNoRows
	WHERE BatchExecutionLogID = @BatchExecutionLogID
		AND TaskExecutionLogID = @TaskExecutionLogID
		AND SourceFileDateStamp = @SourceFileDateStamp
END