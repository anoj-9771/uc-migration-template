


CREATE   view [OneEBS].[UI_Links] AS  
Select [INSTITUTION_CONTEXT_ID]
      ,[ID]
      ,[UI_CODE_FROM]
      ,[UI_CODE_TO]
      ,[LINK_TYPE]
      ,[CREATED_BY]
      ,[CREATED_DATE]
      ,[UPDATED_BY]
      ,[UPDATED_DATE]
      ,[START_DATE]
      ,[END_DATE]
      ,[PROGRESS_WEIGHT]
      ,[DEFAULT_PROGRESS_CODE]
      ,[TARGET_WEIGHT]
      ,[IS_CORE]
      ,[IS_EXCLUDED_FROM_AWARD_CALC]
      ,[IS_GRADE_AWARD_OUTCOME_CONTRIB]
      ,[SEQUENCENUMBER] 
from edw.OneEBS_EBS_0165_UI_LINKS 
WHERE        (_RecordDeleted = 0) AND (_RecordCurrent = 1)