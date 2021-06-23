
Create   view [OneEBS].[People_Units_Sts] AS  
Select [INSTITUTION_CONTEXT_ID]
      ,[ID]
      ,[PEOPLE_UNITS_ID]
      ,[PURCHASING_ACTIVITY_SCHEDULE]
      ,[SUBSIDY_ADJUSTMENT]
      ,[CREATED_BY]
      ,[CREATED_DATE]
      ,[UPDATED_BY]
      ,[UPDATED_DATE] 
from edw.OneEBS_EBS_0165_PEOPLE_UNITS_STS 
WHERE        (_RecordDeleted = 0) AND (_RecordCurrent = 1)