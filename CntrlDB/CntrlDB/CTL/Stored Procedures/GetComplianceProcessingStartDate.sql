CREATE PROCEDURE CTL.GetComplianceProcessingStartDate
AS

WITH AllList AS (
SELECT  [SourceObject]
      ,[StartCounter]
	  ,[EndCounter]
	  ,ROW_NUMBER() over (PARTITION BY SourceObject ORDER BY StartCounter desc) RID
  FROM [CTL].[ControlManifest]
  WHERE SourceObject in (
	SELECT SourceName
	FROM CTL.ControlComplianceDependency Compliance
	INNER JOIN CTL.ControlSource Source ON Source.SourceLocation = Compliance.TableName
	)
), LastLoadedTime AS (
	SELECT 
		SourceObject
		,StartCounter
		,EndCounter
	FROM AllList
	WHERE RID IN (1)
)
SELECT
	MIN(StartCounter) AS ProcessingStartTimeStamp, CONVERT(DATE, (MIN(StartCounter))) ProcessingStartDate
	--*
FROM LastLoadedTime