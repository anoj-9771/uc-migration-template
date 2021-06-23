
Create   view [OneEBS].[Tills] AS  
Select [INSTITUTION_CONTEXT_ID]
      ,[LEGACY_TILL_CODE]
      ,[ID]
      ,[PC_NAME]
      ,[PC_DESCRIPTION]
      ,[EFTPOS_TERMINAL_ID]
      ,[CREATED_DATE]
      ,[CREATED_BY]
      ,[UPDATED_DATE]
      ,[UPDATED_BY]
      ,[TILL_CODE]
      ,[ORGANISATION_CODE] 
from edw.OneEBS_EBS_0165_TILLS 
WHERE        (_RecordDeleted = 0) AND (_RecordCurrent = 1)