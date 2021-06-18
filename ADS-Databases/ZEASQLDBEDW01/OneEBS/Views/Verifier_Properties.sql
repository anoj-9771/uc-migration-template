
Create   view [OneEBS].[Verifier_Properties] AS  
Select [ID]
      ,[RV_DOMAIN]
      ,[LOW_VALUE]
      ,[PROPERTY_NAME]
      ,[PROPERTY_VALUE]
      ,[CREATED_BY]
      ,[CREATED_DATE]
      ,[UPDATED_BY]
      ,[UPDATED_DATE]
      ,[INSTITUTION_CONTEXT_ID] 
from edw.OneEBS_EBS_0165_VERIFIER_PROPERTIES 
WHERE        (_RecordDeleted = 0) AND (_RecordCurrent = 1)