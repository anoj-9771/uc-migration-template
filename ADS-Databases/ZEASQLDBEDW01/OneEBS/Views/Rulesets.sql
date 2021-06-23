
Create   view [OneEBS].[Rulesets] AS  
Select [ID]
      ,[TITLE]
      ,[DESCRIPTION]
      ,[CREATED_BY]
      ,[CREATED_DATE]
      ,[UPDATED_BY]
      ,[UPDATED_DATE]
      ,[INSTITUTION_CONTEXT_ID] 
from edw.OneEBS_EBS_0165_RULESETS 
WHERE        (_RecordDeleted = 0) AND (_RecordCurrent = 1)