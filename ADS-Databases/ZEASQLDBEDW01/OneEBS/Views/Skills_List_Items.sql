
Create   view [OneEBS].[Skills_List_Items] AS  
Select [ID]
      ,[SKILLS_LISTS_ID]
      ,[QUALIFICATION_CODE]
      ,[TITLE]
      ,[IS_ACTIVE]
      ,[IS_LOC_AVAIL_FND_SKILLS]
      ,[IS_LOC_AVAIL_APPRENTICESHIP]
      ,[IS_LOC_AVAIL_TRAINEESHIP]
      ,[IS_LOC_AVAIL_TRGT_PRIORITY]
      ,[CREATED_BY]
      ,[CREATED_DATE]
      ,[UPDATED_BY]
      ,[UPDATED_DATE]
      ,[INSTITUTION_CONTEXT_ID] 
from edw.OneEBS_EBS_0165_SKILLS_LIST_ITEMS 
WHERE        (_RecordDeleted = 0) AND (_RecordCurrent = 1)