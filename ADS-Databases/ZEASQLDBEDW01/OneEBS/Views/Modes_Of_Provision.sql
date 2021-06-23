
Create   view [OneEBS].[Modes_Of_Provision] AS  
Select [PROVISION_CODE]
      ,[INSTITUTION_CONTEXT_ID]
      ,[MOA_CODE]
      ,[CREATED_BY]
      ,[CREATED_DATE]
      ,[UPDATED_BY]
      ,[UPDATED_DATE]
      ,[FES_SHORT_DESCRIPTION]
      ,[FES_LONG_DESCRIPTION]
      ,[FES_NOTIONAL_HOURS]
      ,[FES_FTE]
      ,[FES_ACTIVE] 
from edw.OneEBS_EBS_0165_MODES_OF_PROVISION 
WHERE        (_RecordDeleted = 0) AND (_RecordCurrent = 1)