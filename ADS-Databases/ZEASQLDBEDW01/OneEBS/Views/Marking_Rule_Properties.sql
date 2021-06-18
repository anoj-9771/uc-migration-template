
Create   view [OneEBS].[Marking_Rule_Properties] AS  
Select [INSTITUTION_CONTEXT_ID]
      ,[ID]
      ,[MARKING_RULE_ID]
      ,[PROPERTY_NAME]
      ,[PROPERTY_VALUE]
      ,[CREATED_BY]
      ,[CREATED_DATE]
      ,[UPDATED_BY]
      ,[UPDATED_DATE] 
from edw.OneEBS_EBS_0165_MARKING_RULE_PROPERTIES 
WHERE        (_RecordDeleted = 0) AND (_RecordCurrent = 1)