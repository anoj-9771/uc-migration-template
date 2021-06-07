CREATE PROCEDURE [CTL].[UpdateManifestSQLEDW] (
	@BatchExecutionLogID bigint,
	@SourceObject varchar(255),
	@ProcessedToSQLEDW char(1),
	@PipelineRunID varchar(50)
	)
AS

BEGIN

	--Get the Audit Table for Main Table
	DECLARE @AuditTable VARCHAR(100)
	SELECT @AuditTable = A.SourceName
	FROM CTL.ControlSource S 
	LEFT JOIN CTL.ControlSource A ON S.SoftDeleteSource = A.SourceLocation
	WHERE S.SourceName = @SourceObject

	--If there is no Audit Table defined then use the same table as default
	IF @AuditTable IS NULL OR @AuditTable = ''
		SET @AuditTable = @SourceObject

	IF @ProcessedToSQLEDW = '1'
		UPDATE CTL.ControlManifest SET 
		[ProcessedToSQLEDW] = 1
		,SQLEDWPipelineRunID = @PipelineRunID
		WHERE 1 = 1
		AND (SourceObject = @SourceObject OR SourceObject = @AuditTable) --Check the Manifest logs for both the main and Audit table
		AND [ProcessedToSQLEDW] IS NULL
		AND BatchExecutionLogID <= @BatchExecutionLogID

END