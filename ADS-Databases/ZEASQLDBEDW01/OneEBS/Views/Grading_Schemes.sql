
Create   view [OneEBS].[Grading_Schemes] AS  
Select [INSTITUTION_CONTEXT_ID]
      ,[ID]
      ,[CODE]
      ,[GRADING_SCHEME_TYPE]
      ,[DESCRIPTION]
      ,[IS_ACTIVE]
      ,[CREATED_BY]
      ,[CREATED_DATE]
      ,[UPDATED_BY]
      ,[UPDATED_DATE]
      ,[GRADING_SCHEME_SUBTYPE]
      ,[ALLOW_FLOOD_FILL]
      ,[IS_EXCLUDED_FROM_AWARD_CALC] 
from edw.OneEBS_EBS_0165_GRADING_SCHEMES 
WHERE        (_RecordDeleted = 0) AND (_RecordCurrent = 1)