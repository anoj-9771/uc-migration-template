
Create   view [OneEBS].[Fees_List_Waivers] AS  
Select [ID]
      ,[FEE_RECORD_CODE]
      ,[WAIVER_VALUE_NUMBER]
      ,[ATTACHMENT_ID]
      ,[WAIVER_ORDER]
      ,[CUMULATIVE]
      ,[CREATED_BY]
      ,[CREATED_DATE]
      ,[UPDATED_BY]
      ,[UPDATED_DATE]
      ,[EVIDENCE_ACCEPTED]
      ,[NOTES_ID]
      ,[ADJUSTMENT_AMOUNT]
      ,[INSTITUTION_CONTEXT_ID] 
from edw.OneEBS_EBS_0165_FEES_LIST_WAIVERS 
WHERE        (_RecordDeleted = 0) AND (_RecordCurrent = 1)