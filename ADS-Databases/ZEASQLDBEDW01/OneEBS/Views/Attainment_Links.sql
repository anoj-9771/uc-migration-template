
Create   view [OneEBS].[Attainment_Links] AS  
Select [INSTITUTION_CONTEXT_ID]
      ,[ATT_ATTAINMENT_CODE_TO]
      ,[ATT_ATTAINMENT_CODE_FROM]
      ,[CREATED_BY]
      ,[CREATED_DATE]
      ,[UPDATED_BY]
      ,[UPDATED_DATE]
      ,[CONTRIBUTES_TO_AWARD_OUTCOME] 
from edw.OneEBS_EBS_0165_ATTAINMENT_LINKS 
WHERE        (_RecordDeleted = 0) AND (_RecordCurrent = 1)