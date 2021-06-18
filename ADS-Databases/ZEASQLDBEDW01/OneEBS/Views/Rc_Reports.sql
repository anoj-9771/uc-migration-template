

CREATE   view [OneEBS].[Rc_Reports] AS  
Select [INSTITUTION_CONTEXT_ID]
      ,[IS_DIGITAL_SIGNATURE_SUPPORTED]
      ,[IS_BULK_REPORTING_ALLOWED]
      ,[ALTERNATIVE_CONTEXT]
      ,[ID]
      ,[NAME]
      ,[DISPLAY_NAME]
      ,[PATH]
      ,[DESCRIPTION]
      ,[IS_VISIBLE]
      ,[CREATED_BY]
      ,[CREATED_DATE]
      ,[UPDATED_BY]
      ,[UPDATED_DATE]
      ,[AGENT_CONTEXT]
      ,[APPLICATION]
      ,[CONTEXT]
      ,[PRINTER_SELECTION]
      ,[SAVE_FORMAT]
      ,[SUB_CONTEXT]
      ,[GEN_SEQ_NO] 
from edw.OneEBS_EBS_0165_RC_REPORTS 
WHERE        (_RecordDeleted = 0) AND (_RecordCurrent = 1)