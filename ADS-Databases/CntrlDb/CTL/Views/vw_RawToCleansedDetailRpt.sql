CREATE VIEW [CTL].[vw_RawToCleansedDetailRpt]
  AS SELECT 
  LEFT(T2.SourceFileDateStamp, 8) AS SrcFileDate, 
  LEFT(T1.SrcTimestamp, 8) AS LastSrcFileDate, 
  DATEDIFF(
    MINUTE, StartDateTime, EndDateTime
  ) AS ProcessTimeMin, 
  GETDATE() AS RefreshDate, 
  CASE WHEN T1.SrcTimestamp = T2.SourceFileDateStamp THEN 1 ELSE 0 END AS LastFile, 
  CASE WHEN T1.SrcTimestamp = T2.SourceFileDateStamp AND T2.RawToCleansedMatchStatus = 'Passed' THEN 1 ELSE 0 END AS LastUpdatePass, 
  CAST(
    LEFT(T3.SrcTimestamp, 8) AS DATE
  ) AS LastPassDate,  
  CASE WHEN T2.RawToCleansedMatchStatus = 'Passed' THEN 1 ELSE 0 END AS PassFlag,
  T2.* 
FROM 
  (
    SELECT 
      SourceGroup, 
      SourceLocation, 
      MAX(SourceFileDateStamp) AS SrcTimestamp 
    FROM 
      [CTL].[vw_TechRecRawToCleansedRpt] 
    GROUP BY 
      SourceGroup, 
      SourceLocation
  ) T1, 
  [CTL].[vw_TechRecRawToCleansedRpt] T2 
  LEFT OUTER JOIN (
    SELECT 
      SourceGroup, 
      SourceLocation, 
      MAX(SourceFileDateStamp) AS SrcTimestamp 
    FROM 
      CTL.vw_TechRecRawToCleansedRpt 
    WHERE 
      RawToCleansedMatchStatus = 'Passed' 
    GROUP BY 
      SourceGroup, 
      SourceLocation
  ) T3 ON T2.SourceGroup = T3.SourceGroup 
  AND T2.SourceLocation = T3.SourceLocation 
WHERE 
  T1.SourceGroup = T2.SourceGroup 
  AND T1.SourceLocation = T2.SourceLocation

