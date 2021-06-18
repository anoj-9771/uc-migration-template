
Create   view [OneEBS].[Communications] AS  
Select [INSTITUTION_CONTEXT_ID]
      ,[IS_ACTIVE]
      ,[HAS_BEEN_PRINTED]
      ,[ID]
      ,[LEARNER_ID]
      ,[FORMAT]
      ,[RECIPIENT]
      ,[CREATED_BY]
      ,[CREATED_DATE]
      ,[SUBJECT]
      ,[BODY]
      ,[SOURCE_ID]
      ,[SOURCE]
      ,[BLOB_ID]
      ,[PARENT_ID]
      ,[UPDATED_BY]
      ,[UPDATED_DATE]
      ,[FORMAT_TYPE] 
from edw.OneEBS_EBS_0165_COMMUNICATIONS 
WHERE        (_RecordDeleted = 0) AND (_RecordCurrent = 1)