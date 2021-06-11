/*
Post-Deployment Script Template							
--------------------------------------------------------------------------------------
 This file contains SQL statements that will be appended to the build script.		
 Use SQLCMD syntax to include a file in the post-deployment script.			
 Example:      :r .\myfile.sql								
 Use SQLCMD syntax to reference a variable in the post-deployment script.		
 Example:      :setvar TableName MyTable							
               SELECT * FROM [$(TableName)]					
--------------------------------------------------------------------------------------
*/

/*****************INSERT Data*************************
If you need to insert data into a table, please use a pattern like this.
This checks if the record does not exists on the table then inserts it
******************************************************/

/************* ControlStages ***********************************/

INSERT INTO [CTL].[ControlStages] ([StageSequence], [StageName]) SELECT 100, N'Source to Staging'
WHERE NOT EXISTS (SELECT 1 FROM [CTL].[ControlStages] WHERE [StageName] = N'Source to Staging')

INSERT INTO [CTL].[ControlStages] ([StageSequence], [StageName]) SELECT 200, N'Staging to Processing'
WHERE NOT EXISTS (SELECT 1 FROM [CTL].[ControlStages] WHERE [StageName] = N'Staging to Processing')

INSERT INTO [CTL].[ControlStages] ([StageSequence], [StageName]) SELECT 300, N'Processing to Curated'
WHERE NOT EXISTS (SELECT 1 FROM [CTL].[ControlStages] WHERE [StageName] = N'Processing to Curated')

INSERT INTO [CTL].[ControlStages] ([StageSequence], [StageName]) SELECT 400, N'Staging to Curated'
WHERE NOT EXISTS (SELECT 1 FROM [CTL].[ControlStages] WHERE [StageName] = N'Staging to Synapse')

INSERT INTO [CTL].[ControlStages] ([StageSequence], [StageName]) SELECT 500, N'Processing to Synapse'
WHERE NOT EXISTS (SELECT 1 FROM [CTL].[ControlStages] WHERE [StageName] = N'Processing to Synapse')

INSERT INTO [CTL].[ControlStages] ([StageSequence], [StageName]) SELECT 600, N'Curated to Synapse'
WHERE NOT EXISTS (SELECT 1 FROM [CTL].[ControlStages] WHERE [StageName] = N'Curated to Synapse')

INSERT INTO [CTL].[ControlStages] ([StageSequence], [StageName]) SELECT 700, N'Synapse Export'
WHERE NOT EXISTS (SELECT 1 FROM [CTL].[ControlStages] WHERE [StageName] = N'Synapse Export')



IF (
	SELECT DISTINCT p.rows 
	FROM sys.tables t 
	JOIN sys.schemas s ON t.schema_id = s.schema_id
	JOIN sys.partitions p ON t.object_id = p.object_id 
	WHERE s.name = 'CTL' 
	AND t.name = 'ControlTypes'
) = 0
	BEGIN
		SET IDENTITY_INSERT [CTL].[ControlTypes] ON
		INSERT [CTL].[ControlTypes] ([TypeId], [ControlType]) VALUES (1, N'SQL Server')
		INSERT [CTL].[ControlTypes] ([TypeId], [ControlType]) VALUES (2, N'BLOB Storage (csv)')
		INSERT [CTL].[ControlTypes] ([TypeId], [ControlType]) VALUES (3, N'BLOB Storage (json)')
		INSERT [CTL].[ControlTypes] ([TypeId], [ControlType]) VALUES (4, N'BLOB Storage (parquet)')
		INSERT [CTL].[ControlTypes] ([TypeId], [ControlType]) VALUES (5, N'Flat File')
		INSERT [CTL].[ControlTypes] ([TypeId], [ControlType]) VALUES (7, N'XML')
		INSERT [CTL].[ControlTypes] ([TypeId], [ControlType]) VALUES (8, N'Excel')
		SET IDENTITY_INSERT [CTL].[ControlTypes] OFF
	END

IF (
	SELECT DISTINCT p.rows 
	FROM sys.tables t 
	JOIN sys.schemas s ON t.schema_id = s.schema_id
	JOIN sys.partitions p ON t.object_id = p.object_id 
	WHERE s.name = 'CTL' 
	AND t.name = 'ControlDataLoadTypes'
) = 0
	BEGIN
		SET IDENTITY_INSERT [CTL].[ControlDataLoadTypes] ON
		INSERT [CTL].[ControlDataLoadTypes] (DataLoadTypeID, [DataLoadType], [DeltaExtract], [CDCSource], TruncateTarget, UpsertTarget) VALUES (1, N'TRUNCATE-LOAD', 0, 0, 1, 0)
		INSERT [CTL].[ControlDataLoadTypes] (DataLoadTypeID, [DataLoadType], [DeltaExtract], [CDCSource], TruncateTarget, UpsertTarget) VALUES (2, N'FULL-EXTRACT', 0, 0, 0, 1)
		INSERT [CTL].[ControlDataLoadTypes] (DataLoadTypeID, [DataLoadType], [DeltaExtract], [CDCSource], TruncateTarget, UpsertTarget) VALUES (3, N'INCREMENTAL', 1, 0, 0, 1)
		INSERT [CTL].[ControlDataLoadTypes] (DataLoadTypeID, [DataLoadType], [DeltaExtract], [CDCSource], TruncateTarget, UpsertTarget) VALUES (4, N'APPEND', 1, 0, 0, 0)
		INSERT [CTL].[ControlDataLoadTypes] (DataLoadTypeID, [DataLoadType], [DeltaExtract], [CDCSource], TruncateTarget, UpsertTarget) VALUES (5, N'CDC', 1, 1, 0, 0)
		SET IDENTITY_INSERT [CTL].[ControlDataLoadTypes] OFF
	END


	/*************************************************************************
	Post Deployment Update
	If you need to update any data post deployment, please add the scripts below.
	Please ensure that you check for column existence before you execute the script
	as objects in the Post Deployment Scripts are not validated by compiler	
	*************************************************************************/

	--Example Below to update the UseAuditTable column
	/*
	IF EXISTS(SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'CTL' AND TABLE_NAME = 'ControlSource' AND COLUMN_NAME = 'UseAuditTable')
	BEGIN
		UPDATE CTL.ControlSource SET UseAuditTable = 0 WHERE UseAuditTable IS NULL
	END
	*/

  
  GO