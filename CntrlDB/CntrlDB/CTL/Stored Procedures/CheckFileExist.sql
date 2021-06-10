CREATE PROCEDURE [CTL].[CheckFileExist] (
	@FolderName varchar(1000), 
	@FileName varchar(1000))
AS

SELECT count(1) as FileCount
FROM [CTL].[ControlManifest]
WHERE FolderName = @FolderName and
      FileName = @FileName
