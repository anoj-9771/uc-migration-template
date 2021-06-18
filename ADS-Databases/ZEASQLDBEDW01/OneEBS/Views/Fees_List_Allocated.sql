
Create   view [OneEBS].[Fees_List_Allocated] AS  
Select [INSTITUTION_CONTEXT_ID]
      ,[ORIG_RECEIPT_FEE_RECORD_CODE]
      ,[ID]
      ,[FEE_RECORD_CODE]
      ,[FEE_VALUE_NUMBER]
      ,[PERSON_CODE]
      ,[STATUS]
      ,[AMOUNT_TEMP]
      ,[TYPE]
      ,[RECONCILED_BY]
      ,[RECONCILED_DATE]
      ,[CREATED_BY]
      ,[CREATED_DATE]
      ,[UPDATED_BY]
      ,[UPDATED_DATE]
      ,[AMOUNT]
      ,[VAT_CODE]
      ,[VAT_AMOUNT]
      ,[VAT_RATE] 
from edw.OneEBS_EBS_0165_FEES_LIST_ALLOCATED 
WHERE        (_RecordDeleted = 0) AND (_RecordCurrent = 1)