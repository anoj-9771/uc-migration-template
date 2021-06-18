
Create   view [OneEBS].[Fee_Type_Waivers] AS  
Select [INSTITUTION_CONTEXT_ID]
      ,[FEE_TYPE]
      ,[WAIVER_CODE]
      ,[WAIVER_ORDER]
      ,[CUMULATIVE]
      ,[CREATED_BY]
      ,[CREATED_DATE]
      ,[UPDATED_BY]
      ,[UPDATED_DATE]
      ,[ID] 
from edw.OneEBS_EBS_0165_FEE_TYPE_WAIVERS 
WHERE        (_RecordDeleted = 0) AND (_RecordCurrent = 1)