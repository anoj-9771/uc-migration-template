﻿-- /*
-- Post-Deployment Script Template							
-- --------------------------------------------------------------------------------------
--  This file contains SQL statements that will be appended to the build script.		
--  Use SQLCMD syntax to include a file in the post-deployment script.			
--  Example:      :r .\myfile.sql								
--  Use SQLCMD syntax to reference a variable in the post-deployment script.		
--  Example:      :setvar TableName MyTable							
--                SELECT * FROM [$(TableName)]					
-- --------------------------------------------------------------------------------------
-- */

-- /*****************INSERT Data*************************
-- If you need to insert data into a table, please use a pattern like this.
-- This checks if the record does not exists on the table then inserts it
-- ******************************************************/

-- /************* ControlStages ***********************************/

-- DELETE FROM CTL.ControlProjects
-- --DELETE FROM CTL.ControlProjectSchedule
-- DBCC CHECKIDENT ('CTL.ControlProjects',Reseed,0)
-- --DBCC CHECKIDENT ('CTL.ControlProjectSchedule',Reseed,0)

-- insert into [CTL].[ControlProjects]([ProjectName],[Enabled],[RunSequence]) values('RAW REF ACCESS',1,10);
-- insert into [CTL].[ControlProjects]([ProjectName],[Enabled],[RunSequence]) values('RAW DATA ACCESS',1,20);
-- insert into [CTL].[ControlProjects]([ProjectName],[Enabled],[RunSequence]) values('CLEANSED REF ACCESS',1,30);
-- insert into [CTL].[ControlProjects]([ProjectName],[Enabled],[RunSequence]) values('CLEANSED DATA ACCESS',1,40);
-- insert into [CTL].[ControlProjects]([ProjectName],[Enabled],[RunSequence]) values('HYDRA DATA',1,10);
-- insert into [CTL].[ControlProjects]([ProjectName],[Enabled],[RunSequence]) values('RAW REF CRM',1,10);
-- insert into [CTL].[ControlProjects]([ProjectName],[Enabled],[RunSequence]) values('RAW DATA CRM',1,20);
-- insert into [CTL].[ControlProjects]([ProjectName],[Enabled],[RunSequence]) values('CLEANSED REF CRM',1,30);
-- insert into [CTL].[ControlProjects]([ProjectName],[Enabled],[RunSequence]) values('CLEANSED DATA CRM',1,40);
-- insert into [CTL].[ControlProjects]([ProjectName],[Enabled],[RunSequence]) values('RAW REF ISU',1,10);
-- insert into [CTL].[ControlProjects]([ProjectName],[Enabled],[RunSequence]) values('RAW DATA ISU',1,20);
-- insert into [CTL].[ControlProjects]([ProjectName],[Enabled],[RunSequence]) values('CLEANSED REF ISU',1,30);
-- insert into [CTL].[ControlProjects]([ProjectName],[Enabled],[RunSequence]) values('CLEANSED DATA ISU',1,40);
-- insert into [CTL].[ControlProjects]([ProjectName],[Enabled],[RunSequence]) values('CURATED MASTER',1,50);
-- insert into [CTL].[ControlProjects]([ProjectName],[Enabled],[RunSequence]) values('CURATED BRIDGE',1,60);
-- insert into [CTL].[ControlProjects]([ProjectName],[Enabled],[RunSequence]) values('RAW SLT ISU CHZ2',1,10);
-- insert into [CTL].[ControlProjects]([ProjectName],[Enabled],[RunSequence]) values('RAW SLT ISU Z1',1,20);
-- insert into [CTL].[ControlProjects]([ProjectName],[Enabled],[RunSequence]) values('RAW SLT ISU Z3',1,30);
-- insert into [CTL].[ControlProjects]([ProjectName],[Enabled],[RunSequence]) values('RAW SLT ISU ADRC',1,40);
-- insert into [CTL].[ControlProjects]([ProjectName],[Enabled],[RunSequence]) values('CLEANSED SLT ISU CHZ2',1,50);
-- insert into [CTL].[ControlProjects]([ProjectName],[Enabled],[RunSequence]) values('CLEANSED SLT ISU Z1',1,60);
-- insert into [CTL].[ControlProjects]([ProjectName],[Enabled],[RunSequence]) values('CLEANSED SLT ISU Z3',1,70);
-- insert into [CTL].[ControlProjects]([ProjectName],[Enabled],[RunSequence]) values('CLEANSED SLT ISU ADRC',1,80);
-- insert into [CTL].[ControlProjects]([ProjectName],[Enabled],[RunSequence]) values('BOM715 DATA',1,10);
-- insert into [CTL].[ControlProjects]([ProjectName],[Enabled],[RunSequence]) values('IOT SW TELEMETRY ALARM DATA',1,10);
-- insert into [CTL].[ControlProjects]([ProjectName],[Enabled],[RunSequence]) values('RAW REF HYDRA',1,10);
-- insert into [CTL].[ControlProjects]([ProjectName],[Enabled],[RunSequence]) values('RAW DATA HYDRA',1,20);
-- insert into [CTL].[ControlProjects]([ProjectName],[Enabled],[RunSequence]) values('CLEANSED REF HYDRA',1,30);
-- insert into [CTL].[ControlProjects]([ProjectName],[Enabled],[RunSequence]) values('CLEANSED DATA HYDRA',1,40);
-- insert into [CTL].[ControlProjects]([ProjectName],[Enabled],[RunSequence]) values('SEMANTIC MASTER',1,70);
-- insert into [CTL].[ControlProjects]([ProjectName],[Enabled],[RunSequence]) values('CURATED BATCH 1',1,10);
-- insert into [CTL].[ControlProjects]([ProjectName],[Enabled],[RunSequence]) values('CURATED BATCH 2',1,20);
-- insert into [CTL].[ControlProjects]([ProjectName],[Enabled],[RunSequence]) values('CURATED BATCH 3',1,30);
-- insert into [CTL].[ControlProjects]([ProjectName],[Enabled],[RunSequence]) values('CURATED BATCH 4',1,40);
-- insert into [CTL].[ControlProjects]([ProjectName],[Enabled],[RunSequence]) values('CURATED BATCH 5',1,50);
-- insert into [CTL].[ControlProjects]([ProjectName],[Enabled],[RunSequence]) values('CURATED BATCH 6',1,60);
-- insert into [CTL].[ControlProjects]([ProjectName],[Enabled],[RunSequence]) values('RAW SLT ISU',1,10);
-- insert into [CTL].[ControlProjects]([ProjectName],[Enabled],[RunSequence]) values('CLEANSED SLT ISU',1,20);
-- insert into [CTL].[ControlProjects]([ProjectName],[Enabled],[RunSequence]) values('CURATED BATCH 7',1,70);

-- INSERT INTO [CTL].[ControlStages] ([StageSequence], [StageName]) SELECT 100, N'Source to Raw'
-- WHERE NOT EXISTS (SELECT 1 FROM [CTL].[ControlStages] WHERE [StageName] = N'Source to Raw')

-- INSERT INTO [CTL].[ControlStages] ([StageSequence], [StageName]) SELECT 200, N'Raw to Cleansed'
-- WHERE NOT EXISTS (SELECT 1 FROM [CTL].[ControlStages] WHERE [StageName] = N'Raw to Cleansed')

-- INSERT INTO [CTL].[ControlStages] ([StageSequence], [StageName]) SELECT 300, N'Cleansed to Curated'
-- WHERE NOT EXISTS (SELECT 1 FROM [CTL].[ControlStages] WHERE [StageName] = N'Cleansed to Curated')

-- IF (
-- 	SELECT DISTINCT p.rows 
-- 	FROM sys.tables t 
-- 	JOIN sys.schemas s ON t.schema_id = s.schema_id
-- 	JOIN sys.partitions p ON t.object_id = p.object_id 
-- 	WHERE s.name = 'CTL' 
-- 	AND t.name = 'ControlTypes'
-- ) = 0
-- 	BEGIN
-- 		SET IDENTITY_INSERT [CTL].[ControlTypes] ON
-- 		INSERT [CTL].[ControlTypes] ([TypeId], [ControlType]) VALUES (1, N'SQL Server')
-- 		INSERT [CTL].[ControlTypes] ([TypeId], [ControlType]) VALUES (2, N'BLOB Storage (csv)')
-- 		INSERT [CTL].[ControlTypes] ([TypeId], [ControlType]) VALUES (3, N'BLOB Storage (json)')
-- 		INSERT [CTL].[ControlTypes] ([TypeId], [ControlType]) VALUES (4, N'BLOB Storage (parquet)')
-- 		INSERT [CTL].[ControlTypes] ([TypeId], [ControlType]) VALUES (5, N'Flat File')
-- 		INSERT [CTL].[ControlTypes] ([TypeId], [ControlType]) VALUES (6, N'Oracle')
-- 		INSERT [CTL].[ControlTypes] ([TypeId], [ControlType]) VALUES (7, N'XML')
-- 		INSERT [CTL].[ControlTypes] ([TypeId], [ControlType]) VALUES (8, N'Databricks')
-- 		INSERT [CTL].[ControlTypes] ([TypeId], [ControlType]) VALUES (9, N'ODBC')
-- 		INSERT [CTL].[ControlTypes] ([TypeId], [ControlType]) VALUES (10, N'Excel')
-- 		INSERT [CTL].[ControlTypes] ([TypeId], [ControlType]) VALUES (11, N'OData-Basic')
-- 		INSERT [CTL].[ControlTypes] ([TypeId], [ControlType]) VALUES (12, N'OData-AADServicePrincipal')
-- 		INSERT [CTL].[ControlTypes] ([TypeId], [ControlType]) VALUES (13, N'SharePoint')
-- 		INSERT [CTL].[ControlTypes] ([TypeId], [ControlType]) VALUES (14, N'BLOB Storage (nc)')
-- 		SET IDENTITY_INSERT [CTL].[ControlTypes] OFF
-- 	END

-- IF (
-- 	SELECT DISTINCT p.rows 
-- 	FROM sys.tables t 
-- 	JOIN sys.schemas s ON t.schema_id = s.schema_id
-- 	JOIN sys.partitions p ON t.object_id = p.object_id 
-- 	WHERE s.name = 'CTL' 
-- 	AND t.name = 'ControlDataLoadTypes'
-- ) = 0
-- 	BEGIN
-- 		SET IDENTITY_INSERT [CTL].[ControlDataLoadTypes] ON
-- 			INSERT [CTL].[ControlDataLoadTypes] (DataLoadTypeID, [DataLoadType], [DeltaExtract], [CDCSource], TruncateTarget, UpsertTarget, AppendTarget) VALUES (1, N'TRUNCATE-LOAD', 0, 0, 1, 0, 0)
-- 			INSERT [CTL].[ControlDataLoadTypes] (DataLoadTypeID, [DataLoadType], [DeltaExtract], [CDCSource], TruncateTarget, UpsertTarget, AppendTarget) VALUES (2, N'FULL-EXTRACT', 0, 0, 0, 1, 0)
-- 			INSERT [CTL].[ControlDataLoadTypes] (DataLoadTypeID, [DataLoadType], [DeltaExtract], [CDCSource], TruncateTarget, UpsertTarget, AppendTarget) VALUES (3, N'INCREMENTAL', 1, 0, 0, 1, 0)
-- 			INSERT [CTL].[ControlDataLoadTypes] (DataLoadTypeID, [DataLoadType], [DeltaExtract], [CDCSource], TruncateTarget, UpsertTarget, AppendTarget) VALUES (4, N'APPEND', 0, 0, 0, 0, 1)
-- 			INSERT [CTL].[ControlDataLoadTypes] (DataLoadTypeID, [DataLoadType], [DeltaExtract], [CDCSource], TruncateTarget, UpsertTarget, AppendTarget) VALUES (5, N'CDC', 1, 1, 0, 0, 0)
-- 		SET IDENTITY_INSERT [CTL].[ControlDataLoadTypes] OFF
-- 	END

-- 	/*************************************************************************
-- 	Post Deployment Update
-- 	If you need to update any data post deployment, please add the scripts below.
-- 	Please ensure that you check for column existence before you execute the script
-- 	as objects in the Post Deployment Scripts are not validated by compiler	
-- 	*************************************************************************/

-- 	--Example Below to update the UseAuditTable column
-- 	/*
-- 	IF EXISTS(SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'CTL' AND TABLE_NAME = 'ControlSource' AND COLUMN_NAME = 'UseAuditTable')
-- 	BEGIN
-- 		UPDATE CTL.ControlSource SET UseAuditTable = 0 WHERE UseAuditTable IS NULL
-- 	END
-- 	*/

--   UPDATE CTL.ControlTasks SET LoadToSqlEDW = 1 WHERE LoadToSqlEDW IS NULL

--   UPDATE CTL.ControlSource SET AdditionalProperty = '' WHERE AdditionalProperty IS NULL
--   UPDATE CTL.ControlSource SET SoftDeleteSource = '' WHERE SoftDeleteSource IS NULL
--   UPDATE CTL.ControlSource SET IsAuditTable = 0 WHERE IsAuditTable IS NULL
--   UPDATE CTL.ControlSource SET UseAuditTable = 0 WHERE UseAuditTable IS NULL
  
--   UPDATE CTL.ControlSource SET SourceGroup = SUBSTRING(SourceName, 0, charindex('_', SourceName)) WHERE SourceGroup IS NULL

--   UPDATE CTL.ControlProjectSchedule SET StageEnabled = 1 WHERE StageEnabled IS NULL

-- DELETE FROM CTL.BusinessRecConfig;

-- INSERT INTO [CTL].[BusinessRecConfig]([BusinessReconGroup], [MeasureID], [MeasureName], [TargetObject], [TargetQuery], [Enabled])
--      VALUES ('Water Consumption Reconciliation', 'Total', 'BILLING_DOC_COUNT', 'BilledWaterConsumption', 'select count(distinct fact.meterconsumptionbillingdocumentSK) as TargetMeasure from curated.factbilledwaterconsumption fact, curated.dimmeterconsumptionbillingdocument dim where dim.meterconsumptionbillingdocumentSK = fact.meterconsumptionbillingdocumentSK and dim.isOutsortedFlag = ''N''', 1)
-- ;

-- INSERT INTO [CTL].[BusinessRecConfig] ([BusinessReconGroup], [MeasureID], [MeasureName], [TargetObject], [TargetQuery], [Enabled])
--      VALUES ('Water Consumption Reconciliation', 'Total', 'CONSUMPTION', 'BilledWaterConsumption', 'select sum(fact.meteredWaterConsumption) as TargetMeasure from curated.factbilledwaterconsumption fact, curated.dimmeterconsumptionbillingdocument dim where dim.meterconsumptionbillingdocumentSK = fact.meterconsumptionbillingdocumentSK and dim.isOutsortedFlag = ''N''', 1)
-- ;

-- INSERT INTO [CTL].[BusinessRecConfig]([BusinessReconGroup], [MeasureID], [MeasureName], [TargetObject], [TargetQuery], [Enabled])
--      VALUES ('Water Consumption Reconciliation', '1000', 'BILLING_DOC_COUNT', 'BilledWaterConsumption', 'select count(distinct fact.meterconsumptionbillingdocumentSK) as TargetMeasure from curated.factbilledwaterconsumption fact, curated.dimmeterconsumptionbillingdocument dimBillDoc, curated.dimmeter dimMeter where dimBillDoc.meterconsumptionbillingdocumentSK = fact.meterconsumptionbillingdocumentSK and fact.meterSK = dimmeter.meterSK and dimBillDoc.isOutsortedFlag = ''N'' and dimmeter.watertype in (''Drinking Water'') and dimmeter.usagemetertype in (''Water Meter'') group by dimmeter.watertype, dimmeter.usagemetertype', 1)
-- ;

-- INSERT INTO [CTL].[BusinessRecConfig] ([BusinessReconGroup], [MeasureID], [MeasureName], [TargetObject], [TargetQuery], [Enabled])
--      VALUES ('Water Consumption Reconciliation', '1000', 'CONSUMPTION', 'BilledWaterConsumption', 'select sum(fact.meteredWaterConsumption) as TargetMeasure from curated.factbilledwaterconsumption fact, curated.dimmeterconsumptionbillingdocument dimBillDoc, curated.dimmeter dimMeter where dimBillDoc.meterconsumptionbillingdocumentSK = fact.meterconsumptionbillingdocumentSK and fact.meterSK = dimmeter.meterSK and dimBillDoc.isOutsortedFlag = ''N'' and dimmeter.watertype in (''Drinking Water'') and dimmeter.usagemetertype in (''Water Meter'') group by dimmeter.watertype, dimmeter.usagemetertype', 1)
-- ;

-- INSERT INTO [CTL].[BusinessRecConfig]([BusinessReconGroup], [MeasureID], [MeasureName], [TargetObject], [TargetQuery], [Enabled])
--      VALUES ('Water Consumption Reconciliation', '2000', 'BILLING_DOC_COUNT', 'BilledWaterConsumption', 'select count(distinct fact.meterconsumptionbillingdocumentSK) as TargetMeasure from curated.factbilledwaterconsumption fact, curated.dimmeterconsumptionbillingdocument dimBillDoc, curated.dimmeter dimMeter where dimBillDoc.meterconsumptionbillingdocumentSK = fact.meterconsumptionbillingdocumentSK and fact.meterSK = dimmeter.meterSK and dimBillDoc.isOutsortedFlag = ''N'' and dimmeter.watertype in (''Recycled Water'') and dimmeter.usagemetertype in (''Water Meter'') group by dimmeter.watertype, dimmeter.usagemetertype', 1)
-- ;

-- INSERT INTO [CTL].[BusinessRecConfig] ([BusinessReconGroup], [MeasureID], [MeasureName], [TargetObject], [TargetQuery], [Enabled])
--      VALUES ('Water Consumption Reconciliation', '2000', 'CONSUMPTION', 'BilledWaterConsumption', 'select sum(fact.meteredWaterConsumption) as TargetMeasure from curated.factbilledwaterconsumption fact, curated.dimmeterconsumptionbillingdocument dimBillDoc, curated.dimmeter dimMeter where dimBillDoc.meterconsumptionbillingdocumentSK = fact.meterconsumptionbillingdocumentSK and fact.meterSK = dimmeter.meterSK and dimBillDoc.isOutsortedFlag = ''N'' and dimmeter.watertype in (''Recycled Water'') and dimmeter.usagemetertype in (''Water Meter'') group by dimmeter.watertype, dimmeter.usagemetertype', 1)
-- ;

-- INSERT INTO [CTL].[BusinessRecConfig]([BusinessReconGroup], [MeasureID], [MeasureName], [TargetObject], [TargetQuery], [Enabled])
--      VALUES ('Water Consumption Reconciliation', '9000', 'BILLING_DOC_COUNT', 'BilledWaterConsumption', 'select count(distinct fact.meterconsumptionbillingdocumentSK) as TargetMeasure from curated.factbilledwaterconsumption fact, curated.dimmeterconsumptionbillingdocument dimBillDoc, curated.dimmeter dimMeter where dimBillDoc.meterconsumptionbillingdocumentSK = fact.meterconsumptionbillingdocumentSK and fact.meterSK = dimmeter.meterSK and dimBillDoc.isOutsortedFlag = ''N'' and dimmeter.watertype in (''Drinking Water'') and dimmeter.usagemetertype in (''Customer Standpipe'') group by dimmeter.watertype, dimmeter.usagemetertype', 1)
-- ;

-- INSERT INTO [CTL].[BusinessRecConfig] ([BusinessReconGroup], [MeasureID], [MeasureName], [TargetObject], [TargetQuery], [Enabled])
--      VALUES ('Water Consumption Reconciliation', '9000', 'CONSUMPTION', 'BilledWaterConsumption', 'select sum(fact.meteredWaterConsumption) as TargetMeasure from curated.factbilledwaterconsumption fact, curated.dimmeterconsumptionbillingdocument dimBillDoc, curated.dimmeter dimMeter where dimBillDoc.meterconsumptionbillingdocumentSK = fact.meterconsumptionbillingdocumentSK and fact.meterSK = dimmeter.meterSK and dimBillDoc.isOutsortedFlag = ''N'' and dimmeter.watertype in (''Drinking Water'') and dimmeter.usagemetertype in (''Customer Standpipe'') group by dimmeter.watertype, dimmeter.usagemetertype', 1)
-- ;

-- INSERT INTO [CTL].[BusinessRecConfig]
--            ([BusinessReconGroup]
--            ,[MeasureId]
--            ,[MeasureName]
--            ,[TargetObject]
--            ,[TargetQuery]
--            ,[Enabled])
--      VALUES
--            ('Water Consumption Reconciliation'
--            ,'Total'
--            ,'BILLING_DOC_COUNT'
--            ,'BilledWaterConsumptionDaily'
--            ,'select count(distinct fact.meterconsumptionbillingdocumentSK) as TargetMeasure from curated.factdailyapportionedconsumption fact, curated.dimmeterconsumptionbillingdocument dim where dim.meterconsumptionbillingdocumentSK = fact.meterconsumptionbillingdocumentSK and dim.isOutsortedFlag = ''N'''
--            ,1)
-- GO

-- INSERT INTO [CTL].[BusinessRecConfig]
--            ([BusinessReconGroup]
--            ,[MeasureId]
--            ,[MeasureName]
--            ,[TargetObject]
--            ,[TargetQuery]
--            ,[Enabled])
--      VALUES
--            ('Water Consumption Reconciliation'
--            ,'Total'
--            ,'CONSUMPTION'
--            ,'BilledWaterConsumptionDaily'
--            ,'select sum(fact.dailyApportionedConsumption) as TargetMeasure from curated.factdailyapportionedconsumption fact, curated.dimmeterconsumptionbillingdocument dim where dim.meterconsumptionbillingdocumentSK = fact.meterconsumptionbillingdocumentSK and dim.isOutsortedFlag = ''N'''
--            ,1)
-- GO

-- INSERT INTO [CTL].[BusinessRecConfig]
--            ([BusinessReconGroup]
--            ,[MeasureId]
--            ,[MeasureName]
--            ,[TargetObject]
--            ,[TargetQuery]
--            ,[Enabled])
--      VALUES
--            ('Water Consumption Reconciliation'
--            ,'1000'
--            ,'BILLING_DOC_COUNT'
--            ,'BilledWaterConsumptionDaily'
--            ,'select count(distinct fact.meterconsumptionbillingdocumentSK) as TargetMeasure from curated.factdailyapportionedconsumption fact, curated.dimmeterconsumptionbillingdocument dimBillDoc, curated.dimmeter dimMeter where dimBillDoc.meterconsumptionbillingdocumentSK = fact.meterconsumptionbillingdocumentSK and fact.meterSK = dimmeter.meterSK and dimBillDoc.isOutsortedFlag = ''N'' and dimmeter.watertype in (''Drinking Water'') and dimmeter.usagemetertype in (''Water Meter'') group by dimmeter.watertype, dimmeter.usagemetertype'
--            ,1)
-- GO

-- INSERT INTO [CTL].[BusinessRecConfig]
--            ([BusinessReconGroup]
--            ,[MeasureId]
--            ,[MeasureName]
--            ,[TargetObject]
--            ,[TargetQuery]
--            ,[Enabled])
--      VALUES
--            ('Water Consumption Reconciliation'
--            ,'1000'
--            ,'CONSUMPTION'
--            ,'BilledWaterConsumptionDaily'
--            ,'select sum(fact.dailyApportionedConsumption) as TargetMeasure from curated.factdailyapportionedconsumption fact, curated.dimmeterconsumptionbillingdocument dimBillDoc, curated.dimmeter dimMeter where dimBillDoc.meterconsumptionbillingdocumentSK = fact.meterconsumptionbillingdocumentSK and fact.meterSK = dimmeter.meterSK and dimBillDoc.isOutsortedFlag = ''N'' and dimmeter.watertype in (''Drinking Water'') and dimmeter.usagemetertype in (''Water Meter'') group by dimmeter.watertype, dimmeter.usagemetertype'
--            ,1)
-- GO

-- INSERT INTO [CTL].[BusinessRecConfig]
--            ([BusinessReconGroup]
--            ,[MeasureId]
--            ,[MeasureName]
--            ,[TargetObject]
--            ,[TargetQuery]
--            ,[Enabled])
--      VALUES
--            ('Water Consumption Reconciliation'
--            ,'2000'
--            ,'CONSUMPTION'
--            ,'BilledWaterConsumptionDaily'
--            ,'select sum(fact.dailyApportionedConsumption) as TargetMeasure from curated.factdailyapportionedconsumption fact, curated.dimmeterconsumptionbillingdocument dimBillDoc, curated.dimmeter dimMeter where dimBillDoc.meterconsumptionbillingdocumentSK = fact.meterconsumptionbillingdocumentSK and fact.meterSK = dimmeter.meterSK and dimBillDoc.isOutsortedFlag = ''N'' and dimmeter.watertype in (''Recycled Water'') and dimmeter.usagemetertype in (''Water Meter'') group by dimmeter.watertype, dimmeter.usagemetertype'
--            ,1)
-- GO

-- INSERT INTO [CTL].[BusinessRecConfig]
--            ([BusinessReconGroup]
--            ,[MeasureId]
--            ,[MeasureName]
--            ,[TargetObject]
--            ,[TargetQuery]
--            ,[Enabled])
--      VALUES
--            ('Water Consumption Reconciliation'
--            ,'2000'
--            ,'BILLING_DOC_COUNT'
--            ,'BilledWaterConsumptionDaily'
--            ,'select count(distinct fact.meterconsumptionbillingdocumentSK) as TargetMeasure from curated.factdailyapportionedconsumption fact, curated.dimmeterconsumptionbillingdocument dimBillDoc, curated.dimmeter dimMeter where dimBillDoc.meterconsumptionbillingdocumentSK = fact.meterconsumptionbillingdocumentSK and fact.meterSK = dimmeter.meterSK and dimBillDoc.isOutsortedFlag = ''N'' and dimmeter.watertype in (''Recycled Water'') and dimmeter.usagemetertype in (''Water Meter'') group by dimmeter.watertype, dimmeter.usagemetertype'
--            ,1)
-- GO

-- INSERT INTO [CTL].[BusinessRecConfig]
--            ([BusinessReconGroup]
--            ,[MeasureId]
--            ,[MeasureName]
--            ,[TargetObject]
--            ,[TargetQuery]
--            ,[Enabled])
--      VALUES
--            ('Water Consumption Reconciliation'
--            ,'9000'
--            ,'BILLING_DOC_COUNT'
--            ,'BilledWaterConsumptionDaily'
--            ,'select count(distinct fact.meterconsumptionbillingdocumentSK) as TargetMeasure from curated.factdailyapportionedconsumption fact, curated.dimmeterconsumptionbillingdocument dimBillDoc, curated.dimmeter dimMeter where dimBillDoc.meterconsumptionbillingdocumentSK = fact.meterconsumptionbillingdocumentSK and fact.meterSK = dimmeter.meterSK and dimBillDoc.isOutsortedFlag = ''N'' and dimmeter.watertype in (''Drinking Water'') and dimmeter.usagemetertype in (''Customer Standpipe'') group by dimmeter.watertype, dimmeter.usagemetertype'
--            ,1)
-- GO

-- INSERT INTO [CTL].[BusinessRecConfig]
--            ([BusinessReconGroup]
--            ,[MeasureId]
--            ,[MeasureName]
--            ,[TargetObject]
--            ,[TargetQuery]
--            ,[Enabled])
--      VALUES
--            ('Water Consumption Reconciliation'
--            ,'9000'
--            ,'CONSUMPTION'
--            ,'BilledWaterConsumptionDaily'
--            ,'select sum(fact.dailyApportionedConsumption) as TargetMeasure from curated.factdailyapportionedconsumption fact, curated.dimmeterconsumptionbillingdocument dimBillDoc, curated.dimmeter dimMeter where dimBillDoc.meterconsumptionbillingdocumentSK = fact.meterconsumptionbillingdocumentSK and fact.meterSK = dimmeter.meterSK and dimBillDoc.isOutsortedFlag = ''N'' and dimmeter.watertype in (''Drinking Water'') and dimmeter.usagemetertype in (''Customer Standpipe'') group by dimmeter.watertype, dimmeter.usagemetertype'
--            ,1)
-- GO


-- GO
GO
