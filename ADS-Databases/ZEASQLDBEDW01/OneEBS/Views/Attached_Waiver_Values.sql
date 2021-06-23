
Create   view [OneEBS].[Attached_Waiver_Values] AS  
Select [INSTITUTION_CONTEXT_ID]
      ,[ATTACHMENT_ID]
      ,[CHARGE_LEVEL]
      ,[RUL_CODE]
      ,[REGISTRATION_CODE]
      ,[WAIVER_VALUE_NUMBER]
      ,[CREATED_BY]
      ,[CREATED_DATE]
      ,[UPDATED_BY]
      ,[UPDATED_DATE]
      ,[EVIDENCE_ACCEPTED]
      ,[NOTES_ID]
	   from edw.OneEBS_EBS_0165_ATTACHED_WAIVER_VALUES 
WHERE        (_RecordDeleted = 0) AND (_RecordCurrent = 1)