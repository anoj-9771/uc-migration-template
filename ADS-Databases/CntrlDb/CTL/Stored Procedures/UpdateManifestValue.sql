CREATE PROCEDURE [CTL].[UpdateManifestValue]
	@BatchExecutionLogID bigint,
	@TaskExecutionLogID bigint,
	@SourceFileDateStamp char(14),
	@M_DeltaRecordCount bigint,
	@M_TotalNoRows bigint,
	@M_Message varchar(255)
AS
	
BEGIN
	UPDATE CTL.ControlManifest
	SET M_DeltaRecordCount = @M_DeltaRecordCount,
		M_TotalNoRows = @M_TotalNoRows,
		M_Message = @M_Message
	WHERE BatchExecutionLogID = @BatchExecutionLogID
		AND TaskExecutionLogID = @TaskExecutionLogID
		AND SourceFileDateStamp = @SourceFileDateStamp
END