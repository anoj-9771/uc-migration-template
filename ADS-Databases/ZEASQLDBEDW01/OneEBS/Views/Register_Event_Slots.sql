
Create   view [OneEBS].[Register_Event_Slots] AS  
Select [MAXIMUM_LEARNERS]
      ,[TIMETABLE_PERIODS_ID]
      ,[INSTITUTION_CONTEXT_ID]
      ,[ID]
      ,[REGISTER_EVENT_ID]
      ,[SESSION_CODE]
      ,[STARTDATE]
      ,[ENDDATE]
      ,[BREAK]
      ,[SLOT_STATUS]
      ,[CREATED_BY]
      ,[CREATED_DATE]
      ,[UPDATED_BY]
      ,[UPDATED_DATE]
      ,[REISSUED_BY]
      ,[REISSUED_DATE]
      ,[REISSUED_COUNT]
      ,[IS_ROLL_CALL] 
from edw.OneEBS_EBS_0165_REGISTER_EVENT_SLOTS 
WHERE        (_RecordDeleted = 0) AND (_RecordCurrent = 1)