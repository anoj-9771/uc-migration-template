CREATE VIEW OneEBS.Note_Links
AS
SELECT [CREATED_BY]
      ,[CREATED_BY_PERSON_CODE]
      ,[CREATED_DATE]
      ,[ID]
      ,[INSTITUTION_CONTEXT_ID]
      ,[IS_READ]
      ,[NOTES_ID]
      ,[NOTE_TYPE]
      ,[PARENT_ID]
      ,[PARENT_TABLE]
  FROM [edw].[OneEBS_EBS_0165_NOTE_LINKS]
  WHERE _RecordCurrent = 1 AND _RecordDeleted = 0