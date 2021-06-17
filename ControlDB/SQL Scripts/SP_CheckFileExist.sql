/****** Object:  StoredProcedure [CTL].[GetManifestRecords]    Script Date: 3/11/2021 4:16:14 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
alter PROCEDURE [CTL].[CheckFileExist] (
	@FolderName varchar(1000), 
	@FileName varchar(1000))
AS

SELECT count(1) as FileCount
FROM [CTL].[ControlManifest]
WHERE FolderName = @FolderName and
      FileName = @FileName
