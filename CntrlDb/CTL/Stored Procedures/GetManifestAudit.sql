
CREATE PROCEDURE [CTL].[GetManifestAudit] (
	@SourceObject varchar(100)
)
AS

	SELECT
	ISNULL(MIN(StartCounter),0) AS StartCounter, 
	ISNULL(MAX(EndCounter), 0) AS EndCounter, 
	ISNULL(MAX(DeltaColumn), '''') AS DeltaColumn, 
	ISNULL(COUNT(BatchExecutionLogID), 0) AS Batches 
	FROM CTL.ControlManifest
	WHERE SourceObject = @SourceObject
	AND RecordCountLoaded > 0 
	AND ProcessedToTrustedZone IS NULL 
	AND RecordCountDeltaTable IS NOT NULL