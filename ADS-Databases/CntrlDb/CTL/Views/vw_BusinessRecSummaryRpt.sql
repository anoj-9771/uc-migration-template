CREATE VIEW [CTL].[vw_BusinessRecSummaryRpt]
  AS 
SELECT 
  T1.BusinessRecID, 
  CreatedDateTime, 
  BusinessReconGroup, 
  SourceObject, 
  T2.TargetObject, 
  T2.MeasureId, 
  T2.MeasureName, 
  SourceMeasureValue, 
  TargetMeasureValue, 
  BusinessRecResult, 
  CASE WHEN T2.BusinessRecResult = 'PASS' THEN 1 ELSE 0 END AS PassFlg, 
  CASE WHEN T2.BusinessRecResult = 'FAIL' THEN 1 ELSE 0 END AS FailFlg, 
  ABS(
    COALESCE(TargetMeasureValue, 0) - SourceMeasureValue
  ) AS RecDiff, 
  getdate() AS RefreshDate,
 CAST(SYSDATETIMEOFFSET() AT TIME ZONE 'E. Australia Standard Time' AS smalldatetime) AS RefreshDateTime
  FROM (
    SELECT 
      MAX(BusinessRecID) AS BusinessRecID, 
      TargetObject, 
      MeasureId, 
      MeasureName 
    from 
      CTL.BusinessRecCurated 
    group by 
      TargetObject, 
      MeasureId, 
      MeasureName
  ) T1, 
  CTL.BusinessRecCurated T2 
WHERE 
  T1.BusinessRecID = T2.BusinessRecID 
  AND T1.TargetObject = T2.TargetObject 
  AND T1.MeasureId = T2.MeasureId 
  AND T1.MeasureName = T2.MeasureName
