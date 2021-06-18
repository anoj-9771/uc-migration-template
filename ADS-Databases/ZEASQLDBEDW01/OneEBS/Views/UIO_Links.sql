


CREATE   view [OneEBS].[UIO_Links] AS  
Select [CASCADE_SPECIAL_DETAILS]
      ,[INSTITUTION_CONTEXT_ID]
      ,[UIO_ID_FROM]
      ,[UIO_ID_TO]
      ,[CREATED_BY]
      ,[CREATED_DATE]
      ,[UPDATED_BY]
      ,[UPDATED_DATE]
      ,[CALOCC_CODE_FROM]
      ,[CALOCC_CODE_TO]
      ,[CREATE_APP_UNIT]
      ,[DELIVERY_DATE]
      ,[EXPLODE]
      ,[MANDATORY]
      ,[UI_CODE_FROM]
      ,[UI_CODE_TO]
      ,[WEEK_NUMBER]
      ,[SEQUENCENUMBER]
      ,[IS_AVAILABLE]
      ,[AVAILABLE_FROM]
      ,[AVAILABLE_TO]
      ,[DEFAULT_PROGRESS_CODE]
      ,[IS_CORE] 
from edw.OneEBS_EBS_0165_UIO_LINKS 
WHERE        (_RecordDeleted = 0) AND (_RecordCurrent = 1)