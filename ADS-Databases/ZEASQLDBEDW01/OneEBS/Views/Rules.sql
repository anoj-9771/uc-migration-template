
Create   view [OneEBS].[Rules] AS  
Select [INSTITUTION_CONTEXT_ID]
      ,[ID]
      ,[DESCRIPTION]
      ,[CREATED_BY]
      ,[CREATED_DATE]
      ,[UPDATED_BY]
      ,[UPDATED_DATE]
      ,[RULE] 
from edw.OneEBS_EBS_0165_RULES 
WHERE        (_RecordDeleted = 0) AND (_RecordCurrent = 1)