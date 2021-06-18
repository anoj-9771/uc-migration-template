
Create   view [OneEBS].[Unit_Instance_Awards] AS  
Select [INSTITUTION_CONTEXT_ID]
      ,[UPDATED_DATE]
      ,[ID]
      ,[UNIT_INSTANCE_CODE]
      ,[AWARD_ID]
      ,[MAIN]
      ,[TYPE]
      ,[CREATED_BY]
      ,[CREATED_DATE]
      ,[UPDATED_BY] 
from edw.OneEBS_EBS_0165_UNIT_INSTANCE_AWARDS 
WHERE        (_RecordDeleted = 0) AND (_RecordCurrent = 1)