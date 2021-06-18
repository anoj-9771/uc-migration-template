
Create   view [OneEBS].[Award_Rulesets] AS  
Select [INSTITUTION_CONTEXT_ID]
      ,[ID]
      ,[AWARD_ID]
      ,[RULESET_ID]
      ,[START_DATE]
      ,[END_DATE]
      ,[CREATED_BY]
      ,[CREATED_DATE]
      ,[UPDATED_BY]
      ,[UPDATED_DATE] 
from edw.OneEBS_EBS_0165_AWARD_RULESETS 
WHERE        (_RecordDeleted = 0) AND (_RecordCurrent = 1)