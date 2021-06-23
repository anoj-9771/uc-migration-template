CREATE VIEW OneEBS.People_DeDuplication_Audit
AS
SELECT [CREATED_BY]
      ,[CREATED_DATE]
      ,[DATE_OF_BIRTH]
      ,[EXTERNAL_REF]
      ,[FORENAME]
      ,[ID]
      ,[INSTITUTION_CONTEXT_ID]
      ,[MIDDLE_NAMES]
      ,[REGISTRATION_NO]
      ,[REMOVED_PERSON_CODE]
      ,[RETAINED_PERSON_CODE]
      ,[SEX]
      ,[SURNAME]
      ,[ULI]
  FROM [edw].[OneEBS_EBS_0165_PEOPLE_DEDUPLICATION_AUDIT]
WHERE        (_RecordDeleted = 0) AND (_RecordCurrent = 1)