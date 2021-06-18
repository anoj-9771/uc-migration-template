


Create View [Avetmiss].[DisabilityType]
AS

SELECT [AvetmissDisabilityTypeSK]
      ,[PersonCode] as StudentID
      ,[ReportingYear]
      ,[DisabilityType]
      ,[_DLCuratedZoneTimeStamp]
      ,[_RecordStart]
      ,[_RecordEnd]
      ,[_RecordDeleted]
      ,[_RecordCurrent]
  FROM [compliance].[AvetmissDisabilityType]