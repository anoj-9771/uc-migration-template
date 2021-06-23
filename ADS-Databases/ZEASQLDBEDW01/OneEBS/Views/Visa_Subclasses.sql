
Create   view [OneEBS].[Visa_Subclasses] AS  
Select [IS_FEE_APPLICABLE]
      ,[START_DATE]
      ,[END_DATE]
      ,[CREATED_BY]
      ,[CREATED_DATE]
      ,[UPDATED_BY]
      ,[UPDATED_DATE]
      ,[INSTITUTION_CONTEXT_ID]
      ,[ID]
      ,[CODE]
      ,[DESCRIPTION]
      ,[VISA_TYPE]
      ,[VISA_CLASS]
      ,[IS_ACTIVE]
      ,[IS_TEMPORARY_VISA_HOLDER]
      ,[IS_ENROLMENT_APPLICABLE]
      ,[IS_TUITION_FEE] 
from edw.OneEBS_EBS_0165_VISA_SUBCLASSES 
WHERE        (_RecordDeleted = 0) AND (_RecordCurrent = 1)