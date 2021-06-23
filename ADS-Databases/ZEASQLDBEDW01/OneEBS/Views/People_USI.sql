


CREATE   view [OneEBS].[People_USI] AS  
Select [ID]
      ,[PERSON_CODE]
      ,[USI]
      ,[HAS_USI_BEEN_MANUALLY_VERIFIED]
      ,[DATE_USI_VERIFIED]
      ,[CITY_OF_BIRTH]
      ,[COUNTRY_OF_STUDY]
      ,[CREATED_DATE]
      ,[CREATED_BY]
      ,[UPDATED_DATE]
      ,[UPDATED_BY]
      ,[INSTITUTION_CONTEXT_ID]
      ,[DUPLICATE_USI]
      ,[COUNTRY_OF_RESIDENCE]
      ,[COUNTRY_OF_BIRTH] 
from edw.OneEBS_EBS_0165_PEOPLE_USI 
WHERE        (_RecordDeleted = 0) AND (_RecordCurrent = 1)