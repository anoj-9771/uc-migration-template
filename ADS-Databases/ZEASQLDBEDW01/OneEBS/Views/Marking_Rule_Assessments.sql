
Create   view [OneEBS].[Marking_Rule_Assessments] AS  
Select [INSTITUTION_CONTEXT_ID]
      ,[GRADING_SCHEME_GRADES_ID]
      ,[ID]
      ,[MARKING_RULE_ID]
      ,[ASSESSMENT_ID]
      ,[CREATED_BY]
      ,[CREATED_DATE]
      ,[UPDATED_BY]
      ,[UPDATED_DATE]
      ,[WEIGHTING] 
from edw.OneEBS_EBS_0165_MARKING_RULE_ASSESSMENTS 
WHERE        (_RecordDeleted = 0) AND (_RecordCurrent = 1)