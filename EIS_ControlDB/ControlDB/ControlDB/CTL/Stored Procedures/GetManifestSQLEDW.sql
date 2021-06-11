
CREATE PROCEDURE [CTL].[GetManifestSQLEDW] (
	@SourceObject varchar(100)
)
AS

	--Get the Audit Table for Main Table
	DECLARE @AuditTable VARCHAR(100)
	SELECT @AuditTable = A.SourceName
	FROM CTL.ControlSource S 
	LEFT JOIN CTL.ControlSource A ON S.SoftDeleteSource = A.SourceLocation
	WHERE S.SourceName = @SourceObject

	--If there is no Audit Table defined then use the same table as default
	IF @AuditTable IS NULL OR @AuditTable = ''
		SET @AuditTable = @SourceObject

	SELECT
	ISNULL(MIN(StartCounter),0) AS StartCounter, 
	ISNULL(MAX(EndCounter), 0) AS EndCounter, 
	ISNULL(MAX(DeltaColumn), '''') AS DeltaColumn, 
	ISNULL(COUNT(BatchExecutionLogID), 0) AS Batches 
	FROM CTL.ControlManifest
	WHERE 1 = 1
	AND (SourceObject = @SourceObject OR SourceObject = @AuditTable) --Check the Manifest logs for both the main and Audit table
	AND ProcessedToSQLEDW IS NULL 
	AND ProcessedToTrustedZone IS NOT NULL