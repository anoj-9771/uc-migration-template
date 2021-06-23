
Create   view [OneEBS].[Ruleset_Rules] AS  
Select [ID]
      ,[RULESET_ID]
      ,[RULE_ID]
      ,[CREATED_BY]
      ,[CREATED_DATE]
      ,[UPDATED_BY]
      ,[UPDATED_DATE]
      ,[INSTITUTION_CONTEXT_ID] 
from edw.OneEBS_EBS_0165_RULESET_RULES 
WHERE        (_RecordDeleted = 0) AND (_RecordCurrent = 1)