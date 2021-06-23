
Create   view [OneEBS].[Vat_Rates] AS  
Select [VAT_RATE_CODE]
      ,[VAT_RATE_PERCENTAGE]
      ,[CREATED_BY]
      ,[CREATED_DATE]
      ,[UPDATED_BY]
      ,[UPDATED_DATE]
      ,[FES_SHORT_DESCRIPTION]
      ,[FES_LONG_DESCRIPTION]
      ,[CFACS_VAT_RATE]
      ,[FES_ROUNDING_FACTOR]
      ,[FES_ACTIVE]
      ,[INSTITUTION_CONTEXT_ID]
      ,[IS_DGR_APPLICABLE] 
from edw.OneEBS_EBS_0165_VAT_RATES 
WHERE        (_RecordDeleted = 0) AND (_RecordCurrent = 1)