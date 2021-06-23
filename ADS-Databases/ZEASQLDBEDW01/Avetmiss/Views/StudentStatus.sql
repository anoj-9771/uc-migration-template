


CREATE View [Avetmiss].[StudentStatus]
AS

SELECT [AvetmissStudentStatusSK]
      ,[PersonCode] as StudentID
      ,[ReportingYear]
      ,[StudentStatusCode]
      ,[_DLCuratedZoneTimeStamp]
      ,[_RecordStart]
      ,[_RecordEnd]
      ,[_RecordDeleted]
      ,[_RecordCurrent]
  FROM [compliance].[AvetmissStudentStatus]