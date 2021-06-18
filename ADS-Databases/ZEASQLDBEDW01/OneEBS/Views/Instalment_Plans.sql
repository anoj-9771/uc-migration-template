
Create   view [OneEBS].[Instalment_Plans] AS  
Select [INSTITUTION_CONTEXT_ID]
      ,[ID]
      ,[PLAN_ID]
      ,[PERSON_CODE]
      ,[CREATED_BY]
      ,[CREATED_DATE]
      ,[UPDATED_BY]
      ,[UPDATED_DATE]
      ,[AMOUNT]
      ,[DUE_DATE]
      ,[EPAYMENT_EXTERNAL_REF]
      ,[EPAYMENT_STATUS]
      ,[EPAYMENT_STATUS_CODE]
      ,[EPAYMENT_TRANSACTION_ID]
      ,[INTERNAL_INVOICE_CODE]
      ,[INTERNAL_INVOICE_DATE]
      ,[MATCHED_FEE_RECORD_CODE]
      ,[PAID] 
from edw.OneEBS_EBS_0165_INSTALMENT_PLANS 
WHERE        (_RecordDeleted = 0) AND (_RecordCurrent = 1)