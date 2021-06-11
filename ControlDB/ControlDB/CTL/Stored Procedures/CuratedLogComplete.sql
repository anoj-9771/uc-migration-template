
CREATE PROCEDURE [CTL].[CuratedLogComplete] (
	@BatchExecutionLogID bigint,
	@TaskExecutionLogID bigint)
AS


UPDATE CTL.ControlCuratedManifest
SET LoadStatus = 'COMPLETE',
EndTimeStamp = [CTL].[udf_GetDateLocalTZ]()
WHERE BatchExecutionLogID = @BatchExecutionLogID
	AND TaskExecutionLogID = @TaskExecutionLogID
	AND LoadStatus = 'STARTED'