
Create   view [OneEBS].[Marking_Rules] AS  
Select [INSTITUTION_CONTEXT_ID]
      ,[ID]
      ,[NAME]
      ,[DESCRIPTION]
      ,[MARKING_RULE_TYPE]
      ,[CREATED_BY]
      ,[CREATED_DATE]
      ,[UPDATED_BY]
      ,[UPDATED_DATE]
      ,[RULE_WEIGHTING]
      ,[GRADING_SCHEME_ID]
      ,[MUST_PASS] 
from edw.OneEBS_EBS_0165_MARKING_RULES 
WHERE        (_RecordDeleted = 0) AND (_RecordCurrent = 1)