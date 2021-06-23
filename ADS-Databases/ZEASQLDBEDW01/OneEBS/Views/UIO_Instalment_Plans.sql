



CREATE   view [OneEBS].[UIO_Instalment_Plans] AS  
Select [INSTITUTION_CONTEXT_ID]
      ,[ID]
      ,[UIO_ID]
      ,[START_DATE]
      ,[END_DATE]
      ,[DESCRIPTION]
      ,[INTERVAL]
      ,[NUMBER_OF_SLOTS]
      ,[CREATED_BY]
      ,[CREATED_DATE]
      ,[UPDATED_BY]
      ,[UPDATED_DATE] 
from edw.OneEBS_EBS_0165_UIO_INSTALMENT_PLANS 
WHERE        (_RecordDeleted = 0) AND (_RecordCurrent = 1)