
Create   view [OneEBS].[Award_Attainment_Links] AS  
Select [INSTITUTION_CONTEXT_ID]
      ,[PEOPLE_UNIT_ID]
      ,[AWARD_ID]
      ,[ATTAINMENT_CODE]
      ,[CREATED_BY]
      ,[CREATED_DATE]
      ,[UPDATED_BY]
      ,[UPDATED_DATE]
      ,[CONTRIBUTES_TO_AWARD_OUTCOME] 
from edw.OneEBS_EBS_0165_AWARD_ATTAINMENT_LINKS 
WHERE        (_RecordDeleted = 0) AND (_RecordCurrent = 1)