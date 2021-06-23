
Create   view [OneEBS].[Org_Unit_Links] AS  
Select [INSTITUTION_CONTEXT_ID]
      ,[PRIMARY_ORGANISATION]
      ,[SECONDARY_ORGANISATION]
      ,[CREATED_BY]
      ,[CREATED_DATE]
      ,[UPDATED_BY]
      ,[UPDATED_DATE] 
from edw.OneEBS_EBS_0165_ORG_UNIT_LINKS 
WHERE        (_RecordDeleted = 0) AND (_RecordCurrent = 1)