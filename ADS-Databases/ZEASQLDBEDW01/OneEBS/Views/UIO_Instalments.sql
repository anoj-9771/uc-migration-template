


CREATE   view [OneEBS].[UIO_Instalments] AS  
Select [FIXED_FEE]
      ,[INSTITUTION_CONTEXT_ID]
      ,[ID]
      ,[UIO_INSTALMENT_PLANS_ID]
      ,[DUE_DATE]
      ,[NUMBER_OF_DAYS]
      ,[INSTALMENT_NUMBER]
      ,[PERCENTAGE]
      ,[IS_ACTIVE]
      ,[CREATED_BY]
      ,[CREATED_DATE]
      ,[UPDATED_BY]
      ,[UPDATED_DATE] 
from edw.OneEBS_EBS_0165_UIO_INSTALMENTS 
WHERE        (_RecordDeleted = 0) AND (_RecordCurrent = 1)