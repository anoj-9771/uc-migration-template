
Create   view [OneEBS].[Notes_Topics] AS  
Select [INSTITUTION_CONTEXT_ID]
      ,[ID]
      ,[TOPIC_CODE]
      ,[SHORT_DESCRIPTION]
      ,[LONG_DESCRIPTION]
      ,[LINKED_ROLE_1]
      ,[LINKED_ROLE_2]
      ,[IS_ACTIVE]
      ,[CREATED_BY]
      ,[CREATED_DATE]
      ,[UPDATED_BY]
      ,[UPDATED_DATE] 
from edw.OneEBS_EBS_0165_NOTES_TOPICS 
WHERE        (_RecordDeleted = 0) AND (_RecordCurrent = 1)