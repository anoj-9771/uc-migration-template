
Create   view [OneEBS].[Student_Status_Log] AS  
Select [INSTITUTION_CONTEXT_ID]
      ,[ID]
      ,[PERSON_CODE]
      ,[STUDENT_STATUS_CODE]
      ,[START_DATE]
      ,[END_DATE]
      ,[CREATED_BY]
      ,[CREATED_DATE]
      ,[UPDATED_BY]
      ,[UPDATED_DATE] 
from edw.OneEBS_EBS_0165_STUDENT_STATUS_LOG 
WHERE        (_RecordDeleted = 0) AND (_RecordCurrent = 1)