
Create   view [OneEBS].[Skills_Lists] AS  
Select [ID]
      ,[CODE]
      ,[START_DATE]
      ,[END_DATE]
      ,[CREATED_BY]
      ,[CREATED_DATE]
      ,[UPDATED_BY]
      ,[UPDATED_DATE]
      ,[INSTITUTION_CONTEXT_ID] 
from edw.OneEBS_EBS_0165_SKILLS_LISTS 
WHERE        (_RecordDeleted = 0) AND (_RecordCurrent = 1)