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
IF (
	SELECT p.rows 
	FROM sys.tables t 
	JOIN sys.schemas s ON t.schema_id = s.schema_id
	JOIN sys.partitions p ON t.object_id = p.object_id 
	WHERE s.name = 'CTL' 
	AND t.name = 'ControlStages'
) = 0
	BEGIN
		SET IDENTITY_INSERT [CTL].[ControlStages] ON 
		INSERT [CTL].[ControlStages] ([ControlStageId], [StageSequence], [StageName]) VALUES (1, 100, N'Source to Raw')
		INSERT [CTL].[ControlStages] ([ControlStageId], [StageSequence], [StageName]) VALUES (2, 200, N'Raw to Trusted')
		INSERT [CTL].[ControlStages] ([ControlStageId], [StageSequence], [StageName]) VALUES (3, 300, N'Trusted to Curated')
		INSERT [CTL].[ControlStages] ([ControlStageId], [StageSequence], [StageName]) VALUES (4, 400, N'Staging to DW')
		SET IDENTITY_INSERT [CTL].[ControlStages] OFF
	END

IF (
	SELECT p.rows 
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
		INSERT [CTL].[ControlTypes] ([TypeId], [ControlType]) VALUES (6, N'Oracle')
		INSERT [CTL].[ControlTypes] ([TypeId], [ControlType]) VALUES (7, N'XML')
		INSERT [CTL].[ControlTypes] ([TypeId], [ControlType]) VALUES (8, N'Databricks')
		INSERT [CTL].[ControlTypes] ([TypeId], [ControlType]) VALUES (9, N'ODBC')
		INSERT [CTL].[ControlTypes] ([TypeId], [ControlType]) VALUES (10, N'Excel')
		INSERT [CTL].[ControlTypes] ([TypeId], [ControlType]) VALUES (11, N'OData-Basic')
		INSERT [CTL].[ControlTypes] ([TypeId], [ControlType]) VALUES (12, N'OData-AADServicePrincipal')
		INSERT [CTL].[ControlTypes] ([TypeId], [ControlType]) VALUES (13, N'SharePoint')
		INSERT [CTL].[ControlTypes] ([TypeId], [ControlType]) VALUES (14, N'API-TAFE')
		INSERT [CTL].[ControlTypes] ([TypeId], [ControlType]) VALUES (15, N'API-General')
		SET IDENTITY_INSERT [CTL].[ControlTypes] OFF
	END

IF (
	SELECT p.rows 
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


	
  UPDATE CTL.ControlStages SET StageName = 'Trusted to Curated' where ControlStageId = 3

  UPDATE CTL.ControlTasks SET LoadToSqlEDW = 1 WHERE LoadToSqlEDW IS NULL

  UPDATE CTL.ControlSource SET AdditionalProperty = '' WHERE AdditionalProperty IS NULL
  UPDATE CTL.ControlSource SET SoftDeleteSource = '' WHERE SoftDeleteSource IS NULL
  UPDATE CTL.ControlSource SET IsAuditTable = 0 WHERE IsAuditTable IS NULL
  UPDATE CTL.ControlSource SET UseAuditTable = 0 WHERE UseAuditTable IS NULL
  

GO
