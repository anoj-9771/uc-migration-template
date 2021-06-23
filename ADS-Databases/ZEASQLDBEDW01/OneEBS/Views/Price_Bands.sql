
Create   view [OneEBS].[Price_Bands] AS  
Select [INSTITUTION_CONTEXT_ID]
      ,[PRICE_BAND_ID]
      ,[PRICE_BAND]
      ,[DESCRIPTION]
      ,[CREATED_BY]
      ,[CREATED_DATE]
      ,[UPDATED_BY]
      ,[UPDATED_DATE] 
from edw.OneEBS_EBS_0165_PRICE_BANDS 
WHERE        (_RecordDeleted = 0) AND (_RecordCurrent = 1)