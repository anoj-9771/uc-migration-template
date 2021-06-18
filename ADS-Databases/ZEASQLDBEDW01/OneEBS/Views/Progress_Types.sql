
Create   view [OneEBS].[Progress_Types] AS  
Select [FES_ACTIVE]
      ,[TYPE_CODE]
      ,[STUDENT_STATUS]
      ,[DESCRIPTION]
      ,[INSTITUTION_CONTEXT_ID]
      ,[CREATED_BY]
      ,[CREATED_DATE]
      ,[UPDATED_BY]
      ,[UPDATED_DATE] 
from edw.OneEBS_EBS_0165_PROGRESS_TYPES 
WHERE        (_RecordDeleted = 0) AND (_RecordCurrent = 1)