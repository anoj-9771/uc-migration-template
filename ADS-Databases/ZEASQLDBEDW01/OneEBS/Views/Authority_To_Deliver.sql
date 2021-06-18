

Create   view [OneEBS].[Authority_To_Deliver] AS  

Select [ID]
      ,[NATIONAL_COURSE_CODE]
      ,[LOCATION_CODE]
      ,[CREATED_BY]
      ,[CREATED_DATE]
      ,[UPDATED_BY]
      ,[UPDATED_DATE] 
from [edw].[OneEBS_EBS_0165_AUTHORITY_TO_DELIVER]  
WHERE        (_RecordDeleted = 0) AND (_RecordCurrent = 1)