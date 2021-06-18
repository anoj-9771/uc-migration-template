
Create   view [OneEBS].[People_Unit_Awards] AS  
Select [INSTITUTION_CONTEXT_ID]
      ,[ID]
      ,[PEOPLE_UNITS_ID]
      ,[UNIT_INSTANCE_AWARD_ID]
      ,[AWARD_RULESET_ID]
      ,[CREATED_BY]
      ,[CREATED_DATE]
      ,[UPDATED_BY]
      ,[UPDATED_DATE] 
from edw.OneEBS_EBS_0165_PEOPLE_UNIT_AWARDS 
WHERE        (_RecordDeleted = 0) AND (_RecordCurrent = 1)