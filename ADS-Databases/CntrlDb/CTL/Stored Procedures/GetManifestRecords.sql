CREATE PROCEDURE [CTL].[GetManifestRecords] (
@BatchID bigint)
AS
SELECT [BatchExecutionLogID]
      ,TaskExecutionLogID
      ,[SourceObject]
      ,[Container]
      ,[StartCounter]
      ,[EndCounter]
      ,RecordCountLoaded
      ,[FolderName]
      ,[FileName]
  FROM [CTL].[ControlManifest]
  WHERE BatchExecutionLogID = @BatchID