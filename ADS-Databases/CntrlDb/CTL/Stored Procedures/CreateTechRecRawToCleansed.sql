CREATE PROCEDURE [CTL].[CreateTechRecRawToCleansed]
(
	@BatchExecutionId bigint,
	@TaskExecutionLogId bigint,
	@TaskId bigint,
    @SourceObject varchar(255),
	@TargetName varchar(255),
	@TotalNoRecords bigint
)
AS
BEGIN

	--Get Total Number of records of the source object from the latest processed manifest file
	DECLARE @SourceFileDateStamp char(14), @ManifestTotalNoRecords bigint, @ManifestId bigint
	
	SELECT @SourceFileDateStamp = SourceFileDateStamp, @ManifestTotalNoRecords = M_TotalNoRows, @ManifestId = ManifestID
	FROM(
		SELECT TOP 1 SourceFileDateStamp, M_TotalNoRows, ManifestID
		FROM CTL.ControlManifest M
		WHERE SourceObject = @SourceObject
			AND [ProcessedToCleansedZone] = 1
		ORDER BY SourceFileDateStamp DESC
	)T

	IF @ManifestId IS NOT NULL
	BEGIN
		INSERT INTO CTL.TechRecRawToCleansed(
			BatchExecutionId,
			TaskExecutionLogId,
			TaskId,
			SourceObject,
			TargetName,
			ManifestId,
			ManifestTotalNoRecords,
			TargetTableRowCount,
			SourceFileDateStamp
		)
		VALUES(
			@BatchExecutionId,
			@TaskExecutionLogId,
			@TaskId,
			@SourceObject,
			@TargetName,
			@ManifestId,
			@ManifestTotalNoRecords,
			@TotalNoRecords,
			@SourceFileDateStamp
		)
	END
END
