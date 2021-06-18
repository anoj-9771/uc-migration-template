


CREATE   view [OneEBS].[UI_Target_Audiences] AS  
Select [INSTITUTION_CONTEXT_ID]
      ,[ID]
      ,[UI_ID]
      ,[TARGET_AUDIENCE]
      ,[CREATED_BY]
      ,[CREATED_DATE]
      ,[UPDATED_BY]
      ,[UPDATED_DATE] 
from edw.OneEBS_EBS_0165_UI_TARGET_AUDIENCES 
WHERE        (_RecordDeleted = 0) AND (_RecordCurrent = 1)