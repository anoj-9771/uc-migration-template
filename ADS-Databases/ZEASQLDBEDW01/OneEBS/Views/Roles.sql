CREATE VIEW OneEBS.Roles
AS
SELECT [APP_ID]
      ,[CREATED_BY]
      ,[CREATED_DATE]
      ,[GROUP_ID]
      ,[GROUP_ID2]
      ,[INSTITUTION_CONTEXT_ID]
      ,[OBJECT_ID]
      ,[PERSON_CODE]
      ,[ROLE]
      ,[ROLE_ID]
      ,[TYPE]
      ,[UPDATED_BY]
      ,[UPDATED_DATE]
  FROM [edw].[OneEBS_EBS_0165_ROLES]
WHERE        (_RecordDeleted = 0) AND (_RecordCurrent = 1)