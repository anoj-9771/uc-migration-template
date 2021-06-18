
Create   view [OneEBS].[People_Units_Financial] AS  
Select [ID]
      ,[PEOPLE_UNITS_ID]
      ,[FEE_TYPE_INDICATOR]
      ,[STUDENT_FEE_AMOUNT]
      ,[CREATED_BY]
      ,[CREATED_DATE]
      ,[UPDATED_BY]
      ,[UPDATED_DATE] 
from edw.OneEBS_EBS_0165_PEOPLE_UNITS_FINANCIAL 
WHERE        (_RecordDeleted = 0) AND (_RecordCurrent = 1)