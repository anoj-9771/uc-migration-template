
Create   view [OneEBS].[Awards_Printed_Details] AS  
Select [INSTITUTION_CONTEXT_ID]
      ,[ID]
      ,[ATTAINMENT_CODE]
      ,[TESTAMUR_DATE_LAST_PRINTED]
      ,[TRANSCRIPT_DATE_LAST_PRINTED]
      ,[EMPLOYER_REPORT_LAST_PRINTED]
      ,[HAS_TRANSCRIPT_CHANGED]
      ,[CREATED_BY]
      ,[CREATED_DATE]
      ,[UPDATED_BY]
      ,[UPDATED_DATE] 
from edw.OneEBS_EBS_0165_AWARDS_PRINTED_DETAILS 
WHERE        (_RecordDeleted = 0) AND (_RecordCurrent = 1)