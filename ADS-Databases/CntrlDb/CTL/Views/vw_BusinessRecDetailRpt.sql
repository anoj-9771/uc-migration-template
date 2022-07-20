CREATE VIEW [CTL].[vw_BusinessRecDetailRpt]
  AS 
SELECT 
  *,
  CASE WHEN TargetMeasureValue > 0 THEN ABS(TargetMeasureValue - SourceMeasureValue) ELSE SourceMeasureValue END AS RecDiff,
  GETDATE() AS RefreshDate
FROM
  CTL.BusinessRecCurated
