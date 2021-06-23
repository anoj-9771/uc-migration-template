Create   view [OneEBS].[Z_Read_Batches] AS  
Select [CREATED_DATE]
      ,[UPDATED_BY]
      ,[UPDATED_DATE]
      ,[BANKED_BY]
      ,[PC_NAME]
      ,[BANKING_GROUP_ID]
      ,[ORGANISATION_ID]
      ,[BANKING_DEPOSIT_NO]
      ,[INSTITUTION_CONTEXT_ID]
      ,[DATE_TO_BE_BANKED]
      ,[ID]
      ,[TILL_CODE]
      ,[CREATED_BY] 
from edw.OneEBS_EBS_0165_Z_READ_BATCHES 
WHERE        (_RecordDeleted = 0) AND (_RecordCurrent = 1) 
GO


