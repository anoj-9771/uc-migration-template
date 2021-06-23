


CREATE   view [OneEBS].[UIO_Organisation_Units] AS  
Select [INSTITUTION_CONTEXT_ID]
      ,[ID]
      ,[UIO_ID]
      ,[ORGANISATION_CODE]
      ,[TYPE]
      ,[CREATED_BY]
      ,[CREATED_DATE]
      ,[UPDATED_BY]
      ,[UPDATED_DATE] 
from edw.OneEBS_EBS_0165_UIO_ORGANISATION_UNITS 
WHERE        (_RecordDeleted = 0) AND (_RecordCurrent = 1)