

CREATE   view [OneEBS].[People_UIO] AS  
Select [INSTITUTION_CONTEXT_ID]
      ,[START_DATE]
      ,[END_DATE]
      ,[STATUS]
      ,[PERSON_RIGHT]
      ,[OBS_TYPE]
      ,[CREATED_BY]
      ,[CREATED_DATE]
      ,[UPDATED_BY]
      ,[UPDATED_DATE]
      ,[ID]
      ,[PERSON_CODE]
      ,[UIO_ID]
      ,[TEACH_ALL_LEARNERS]
      ,[TYPE]
      ,[TUTORGROUP_ID] 
from edw.OneEBS_EBS_0165_PEOPLE_UIO 
WHERE        (_RecordDeleted = 0) AND (_RecordCurrent = 1)