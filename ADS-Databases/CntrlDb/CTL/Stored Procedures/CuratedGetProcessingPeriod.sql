CREATE PROCEDURE [CTL].[CuratedGetProcessingPeriod] (
	@SubjectArea varchar(255),
	@Project varchar(255)
	)
AS

--DECLARE @SubjectArea varchar(255) = 'NAT';
--DECLARE @Project varchar(255) = 'ComplianceCurated';

DECLARE @StartDate DATETIME;
DECLARE @EndDate DATETIME;

IF EXISTS(SELECT 1 FROM CTL.ControlCuratedConfig Config WHERE Config.SubjectArea =  @SubjectArea AND Config.Project =  @Project)
BEGIN
	--Check If there are configuration then get the dates from data load information
	WITH AllList AS (
		--Get all the data load entries from ControlManifest for the passed parameter for Curated Entity and Rank them
		SELECT  
			[SourceObject]
			,[StartCounter]
			,[EndCounter]
			,ROW_NUMBER() over (PARTITION BY SourceObject ORDER BY StartCounter desc) RowRank
		FROM [CTL].[ControlManifest]
		WHERE SourceObject in (
			--Check for matching records in the Configuration Table ControlCuratedConfig
			SELECT SourceName
			FROM CTL.ControlCuratedConfig Config
			INNER JOIN CTL.ControlSource Source ON Source.SourceLocation = Config.DependentTableName
			WHERE Config.SubjectArea =  @SubjectArea
			AND Config.Project =  @Project
			AND Config.Valid = 1
			AND ProcessedToCleansedZone = 1
		)
	), LastLoadedTime AS (
		--Take only the latest load for each table
		SELECT 
			SourceObject
			,StartCounter
			,EndCounter
		FROM AllList
		WHERE RowRank IN (1)
	)
	SELECT
		--Get the minimum of each Start Date as Processing Start Time. Default to midnight time
		@StartDate = CONVERT(DATETIME, CONVERT(DATE, (MIN(StartCounter))))
		--Get current datetime at midnight as the Processing End Time
		,@EndDate = CONVERT(DATETIME, CAST(SYSDATETIMEOFFSET() AT TIME ZONE 'AUS Eastern Standard Time' AS date))
		--*
	FROM LastLoadedTime
END
ELSE
BEGIN
	--If there are no configuration then set the default dates
	SET @StartDate = '2000-01-01 00:00:00'
	SET @EndDate = '9999-12-31 00:00:00'
END


SELECT @StartDate AS ProcessingStartDate, @EndDate AS ProcessingEndDate