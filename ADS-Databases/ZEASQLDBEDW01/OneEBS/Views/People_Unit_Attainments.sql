
Create   view [OneEBS].[People_Unit_Attainments] AS  
Select [INSTITUTION_CONTEXT_ID]
      ,[ID]
      ,[PEOPLE_UNITS_ID]
      ,[ATTAINMENT_CODE]
      ,[LINK_TYPE]
      ,[CREATED_BY]
      ,[CREATED_DATE]
      ,[UPDATED_BY]
      ,[UPDATED_DATE] 
from edw.OneEBS_EBS_0165_PEOPLE_UNIT_ATTAINMENTS 
WHERE        (_RecordDeleted = 0) AND (_RecordCurrent = 1)