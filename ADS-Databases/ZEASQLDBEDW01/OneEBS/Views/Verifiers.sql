
Create   view [OneEBS].[Verifiers] AS  
Select [ID]
      ,[LOW_VALUE]
      ,[RV_DOMAIN]
      ,[ABBREVIATION]
      ,[CREATED_BY]
      ,[CREATED_DATE]
      ,[UPDATED_BY]
      ,[UPDATED_DATE]
      ,[FRANCHISING_TYPE]
      ,[OPS_MASTER]
      ,[FES_SHORT_DESCRIPTION]
      ,[FES_LONG_DESCRIPTION]
      ,[SIR_VALUE]
      ,[FES_ACTIVE]
      ,[ROLE_DEFS_ID]
      ,[INSTITUTION_CONTEXT_ID] 
from edw.OneEBS_EBS_0165_VERIFIERS 
WHERE        (_RecordDeleted = 0) AND (_RecordCurrent = 1)