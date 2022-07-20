CREATE VIEW [CTL].[vw_SrcToRawDetailRpt]
  AS SELECT 
  CAST(
    LEFT(T2.SourceFileDateStamp, 8) AS DATE
  ) AS SrcFileDate, 
  CAST(
    LEFT(T1.SrcTimestamp, 8) AS DATE
  ) AS LastSrcFileDate, 
  DATEDIFF(
    DAY, 
    CAST(
      LEFT(T2.SourceFileDateStamp, 8) AS DATE
    ), 
    GETDATE()
  ) AS ProcessTimeDays, 
  DATEDIFF(
    MINUTE, StartDateTime, EndDateTime
  ) AS ProcessTimeMin, 
  T2.SourceGroup + '/' + T2.SourceLocation AS SrcTableID, 
  GETDATE() AS RefreshDate, 
  CASE WHEN T1.SrcTimestamp = T2.SourceFileDateStamp THEN 1 ELSE 0 END AS LastFile, 
  CASE WHEN T1.SrcTimestamp = T2.SourceFileDateStamp 
  AND T2.SrcToRawMatchStatus = 'Passed' THEN 1 ELSE 0 END AS LastUpdatePass, 
  CAST(
    LEFT(T3.SrcTimestamp, 8) AS DATE
  ) AS LastPassDate, 
  CASE WHEN T2.SrcToRawMatchStatus = 'Passed' THEN 1 ELSE 0 END AS PassFlag,
  T2.* ,
  CAST(SYSDATETIMEOFFSET() AT TIME ZONE 'E. Australia Standard Time' AS smalldatetime) AS RefreshDateTime
FROM 
  (
    SELECT 
      SourceGroup, 
      SourceLocation, 
      MAX(SourceFileDateStamp) AS SrcTimestamp 
    FROM 
      CTL.vw_TechRecSrcToRawRpt 
    GROUP BY 
      SourceGroup, 
      SourceLocation
  ) T1, 
  CTL.vw_TechRecSrcToRawRpt T2 
  LEFT OUTER JOIN (
    SELECT 
      SourceGroup, 
      SourceLocation, 
      MAX(SourceFileDateStamp) AS SrcTimestamp 
    FROM 
      CTL.vw_TechRecSrcToRawRpt 
    WHERE 
      SrcToRawMatchStatus = 'Passed' 
    GROUP BY 
      SourceGroup, 
      SourceLocation
  ) T3 ON T2.SourceGroup = T3.SourceGroup 
  AND T2.SourceLocation = T3.SourceLocation 
WHERE 
  T1.SourceGroup = T2.SourceGroup 
  AND T1.SourceLocation = T2.SourceLocation

