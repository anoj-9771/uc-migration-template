
CREATE PROCEDURE CTL.GetManifestTrusted (
	@SourceObject varchar(100)
)
AS
	SELECT
	*
	FROM CTL.ControlManifest
	WHERE SourceObject = @SourceObject
	AND RecordCountLoaded > 0 
	AND ProcessedToTrustedZone IS NULL 
	AND RecordCountDeltaTable IS NOT NULL 
	ORDER BY StartCounter