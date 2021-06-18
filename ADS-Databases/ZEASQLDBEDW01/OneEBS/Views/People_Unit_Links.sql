
Create   view [OneEBS].[People_Unit_Links] AS  
Select [CREATED_DATE]
      ,[INSTITUTION_CONTEXT_ID]
      ,[ID]
      ,[PARENT_ID]
      ,[CHILD_ID]
      ,[MANDATORY]
      ,[CREATED_BY]
      ,[UPDATED_BY]
      ,[UPDATED_DATE]
      ,[IS_ELECTIVE]
from edw.OneEBS_EBS_0165_PEOPLE_UNIT_LINKS 
WHERE        (_RecordDeleted = 0) AND (_RecordCurrent = 1)