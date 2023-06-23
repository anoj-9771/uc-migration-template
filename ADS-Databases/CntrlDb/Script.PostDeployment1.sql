DECLARE @SummaryOfChanges TABLE(Change VARCHAR(20));
WITH [_Common] AS
(
    SELECT *
    FROM (
        SELECT 'IngestionDefault' [KeyGroup], 'RawPath' [Key], '/raw/$SYSTEM$/$SCHEMA$_$TABLE$/$yyyy$/$MM$/$dd$/$HH$$mm$/$guid$.$EXT$' [Value]
        UNION SELECT 'IngestionDefault' [KeyGroup], 'CleansedPath' [Key], '/cleansed/$SYSTEM$/$SCHEMA$_$TABLE$/' [Value]
        UNION SELECT 'IngestionDefault' [KeyGroup], 'SourceKeyVaultSecret' [Key], 'daf-sa-lake-connectionstring' [Value]
        UNION SELECT 'IngestionDefault' [KeyGroup], 'Enabled' [Key], '1' [Value]
        UNION SELECT 'IngestionSheet' [KeyGroup], 'Index' [Key], '0' [Value]
        UNION SELECT 'IngestionSheet' [KeyGroup], 'Limit' [Key], '0' [Value]
        UNION SELECT 'IngestionSheet' [KeyGroup], 'Path' [Key], 'testonly/labware_upload.csv' [Value]
        UNION SELECT 'TriggerInterval' [KeyGroup], '15Min' [Key], 'maximo|15min,iicats|15min,scada|15min,hydstra|15Min' [Value]
    ) T
),
[_Env] AS
(
    SELECT Value from dbo.config where [KeyGroup] = 'System' and [Key] = 'Environment' 
),
[_Override] AS
(
    SELECT *
    FROM (
        SELECT 'cleansedLayer' [KeyGroup], 'skipIngestion' [Key], '' [Value]
    ) O
    WHERE EXISTS(SELECT * FROM [_Env] WHERE VALUE = 'PROD')
),
[_SourceConfig] AS
(
    SELECT  COALESCE([_Override].[KeyGroup], [_Common].[KeyGroup]) [KeyGroup]
        ,COALESCE([_Override].[Key], [_Common].[Key]) [Key]
        ,COALESCE([_Override].[Value], [_Common].[Value]) [Value]
        ,CONVERT(DATETIME, CONVERT(DATETIMEOFFSET, GETDATE()) AT TIME ZONE 'AUS Eastern Standard Time') [CreatedDTS]
    FROM [_Common]
    FULL JOIN [_Override]
    ON ([_Common].[KeyGroup] = [_Override].[KeyGroup] and [_Common].[Key] = [_Override].[Key])
)
MERGE INTO [dbo].[Config] AS TGT
USING [_SourceConfig]
AS SRC ON TGT.[KeyGroup] = SRC.[KeyGroup] AND TGT.[Key] = SRC.[Key]
WHEN NOT MATCHED BY TARGET THEN
    INSERT ([KeyGroup],[Key],[Value],[CreatedDTS]) VALUES ([KeyGroup],[Key],[Value],[CreatedDTS])
WHEN MATCHED THEN
	UPDATE SET [Value] = SRC.[Value]    
OUTPUT $action 
    INTO @SummaryOfChanges;
SELECT * FROM @SummaryOfChanges;


-- /*
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

-- insert into [CTL].[ControlProjects]([ProjectName],[Enabled],[RunSequence]) values('RAW ACCESS',1,10);
-- insert into [CTL].[ControlProjects]([ProjectName],[Enabled],[RunSequence]) values('CLEANSED ACCESS REF',1,30);
-- insert into [CTL].[ControlProjects]([ProjectName],[Enabled],[RunSequence]) values('CLEANSED ACCESS DATA',1,40);
-- insert into [CTL].[ControlProjects]([ProjectName],[Enabled],[RunSequence]) values('RAW HYDRA',1,10);
-- insert into [CTL].[ControlProjects]([ProjectName],[Enabled],[RunSequence]) values('CLEANSED HYDRA REF',1,30);
-- insert into [CTL].[ControlProjects]([ProjectName],[Enabled],[RunSequence]) values('CLEANSED HYDRA DATA',1,40);
-- insert into [CTL].[ControlProjects]([ProjectName],[Enabled],[RunSequence]) values('RAW CRM',1,10);
-- insert into [CTL].[ControlProjects]([ProjectName],[Enabled],[RunSequence]) values('CLEANSED CRM REF',1,30);
-- insert into [CTL].[ControlProjects]([ProjectName],[Enabled],[RunSequence]) values('CLEANSED CRM DATA',1,40);
-- insert into [CTL].[ControlProjects]([ProjectName],[Enabled],[RunSequence]) values('RAW ISU',1,10);
-- insert into [CTL].[ControlProjects]([ProjectName],[Enabled],[RunSequence]) values('CLEANSED ISU REF',1,30);
-- insert into [CTL].[ControlProjects]([ProjectName],[Enabled],[RunSequence]) values('CLEANSED ISU DATA',1,40);
-- insert into [CTL].[ControlProjects]([ProjectName],[Enabled],[RunSequence]) values('RAW ISU SLT',1,10);
-- insert into [CTL].[ControlProjects]([ProjectName],[Enabled],[RunSequence]) values('CLEANSED ISU SLT',1,20);
-- insert into [CTL].[ControlProjects]([ProjectName],[Enabled],[RunSequence]) values('CURATED BATCH 1',1,10);
-- insert into [CTL].[ControlProjects]([ProjectName],[Enabled],[RunSequence]) values('CURATED BATCH 2',1,20);
-- insert into [CTL].[ControlProjects]([ProjectName],[Enabled],[RunSequence]) values('CURATED BATCH 3',1,30);
-- insert into [CTL].[ControlProjects]([ProjectName],[Enabled],[RunSequence]) values('CURATED BATCH 4',1,40);
-- insert into [CTL].[ControlProjects]([ProjectName],[Enabled],[RunSequence]) values('CURATED BATCH 5',1,50);
-- insert into [CTL].[ControlProjects]([ProjectName],[Enabled],[RunSequence]) values('CURATED BATCH 6',1,60);
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

-- SET IDENTITY_INSERT [CTL].[BusinessRecConfig] ON 
-- GO
-- INSERT [CTL].[BusinessRecConfig] ([BusinessRecConfigId], [BusinessReconGroup], [MeasureId], [MeasureName], [TargetObject], [TargetQuery], [Enabled]) VALUES (1, N'Water Consumption Reconciliation', N'Total', N'BILLING_DOC_COUNT', N'BilledWaterConsumption', N'select count(distinct fact.meterconsumptionbillingdocumentSK) as TargetMeasure from curated.factbilledwaterconsumption fact, curated.dimmeterconsumptionbillingdocument dim where fact.sourceSystemCode = ''ISU'' and dim.meterconsumptionbillingdocumentSK = fact.meterconsumptionbillingdocumentSK and dim.isOutsortedFlag = ''N''', 1)
-- GO
-- INSERT [CTL].[BusinessRecConfig] ([BusinessRecConfigId], [BusinessReconGroup], [MeasureId], [MeasureName], [TargetObject], [TargetQuery], [Enabled]) VALUES (2, N'Water Consumption Reconciliation', N'Total', N'CONSUMPTION', N'BilledWaterConsumption', N'select sum(fact.meteredWaterConsumption) as TargetMeasure from curated.factbilledwaterconsumption fact, curated.dimmeterconsumptionbillingdocument dim where fact.sourceSystemCode = ''ISU'' and dim.meterconsumptionbillingdocumentSK = fact.meterconsumptionbillingdocumentSK and dim.isOutsortedFlag = ''N''', 1)
-- GO
-- INSERT [CTL].[BusinessRecConfig] ([BusinessRecConfigId], [BusinessReconGroup], [MeasureId], [MeasureName], [TargetObject], [TargetQuery], [Enabled]) VALUES (3, N'Water Consumption Reconciliation', N'1000', N'BILLING_DOC_COUNT', N'BilledWaterConsumption', N'select count(distinct fact.meterconsumptionbillingdocumentSK) as TargetMeasure from curated.factbilledwaterconsumption fact, curated.dimmeterconsumptionbillingdocument dimBillDoc, curated.dimmeter dimMeter where fact.sourceSystemCode = ''ISU'' and dimBillDoc.meterconsumptionbillingdocumentSK = fact.meterconsumptionbillingdocumentSK and fact.meterSK = dimmeter.meterSK and dimBillDoc.isOutsortedFlag = ''N'' and dimmeter.watertype in (''Drinking Water'') and dimmeter.usagemetertype in (''Water Meter'') group by dimmeter.watertype, dimmeter.usagemetertype', 1)
-- GO
-- INSERT [CTL].[BusinessRecConfig] ([BusinessRecConfigId], [BusinessReconGroup], [MeasureId], [MeasureName], [TargetObject], [TargetQuery], [Enabled]) VALUES (4, N'Water Consumption Reconciliation', N'1000', N'CONSUMPTION', N'BilledWaterConsumption', N'select sum(fact.meteredWaterConsumption) as TargetMeasure from curated.factbilledwaterconsumption fact, curated.dimmeterconsumptionbillingdocument dimBillDoc, curated.dimmeter dimMeter where fact.sourceSystemCode = ''ISU'' and dimBillDoc.meterconsumptionbillingdocumentSK = fact.meterconsumptionbillingdocumentSK and fact.meterSK = dimmeter.meterSK and dimBillDoc.isOutsortedFlag = ''N'' and dimmeter.watertype in (''Drinking Water'') and dimmeter.usagemetertype in (''Water Meter'') group by dimmeter.watertype, dimmeter.usagemetertype', 1)
-- GO
-- INSERT [CTL].[BusinessRecConfig] ([BusinessRecConfigId], [BusinessReconGroup], [MeasureId], [MeasureName], [TargetObject], [TargetQuery], [Enabled]) VALUES (5, N'Water Consumption Reconciliation', N'2000', N'BILLING_DOC_COUNT', N'BilledWaterConsumption', N'select count(distinct fact.meterconsumptionbillingdocumentSK) as TargetMeasure from curated.factbilledwaterconsumption fact, curated.dimmeterconsumptionbillingdocument dimBillDoc, curated.dimmeter dimMeter where fact.sourceSystemCode = ''ISU'' and dimBillDoc.meterconsumptionbillingdocumentSK = fact.meterconsumptionbillingdocumentSK and fact.meterSK = dimmeter.meterSK and dimBillDoc.isOutsortedFlag = ''N'' and dimmeter.watertype in (''Recycled Water'') and dimmeter.usagemetertype in (''Water Meter'') group by dimmeter.watertype, dimmeter.usagemetertype', 1)
-- GO
-- INSERT [CTL].[BusinessRecConfig] ([BusinessRecConfigId], [BusinessReconGroup], [MeasureId], [MeasureName], [TargetObject], [TargetQuery], [Enabled]) VALUES (6, N'Water Consumption Reconciliation', N'2000', N'CONSUMPTION', N'BilledWaterConsumption', N'select sum(fact.meteredWaterConsumption) as TargetMeasure from curated.factbilledwaterconsumption fact, curated.dimmeterconsumptionbillingdocument dimBillDoc, curated.dimmeter dimMeter where fact.sourceSystemCode = ''ISU'' and dimBillDoc.meterconsumptionbillingdocumentSK = fact.meterconsumptionbillingdocumentSK and fact.meterSK = dimmeter.meterSK and dimBillDoc.isOutsortedFlag = ''N'' and dimmeter.watertype in (''Recycled Water'') and dimmeter.usagemetertype in (''Water Meter'') group by dimmeter.watertype, dimmeter.usagemetertype', 1)
-- GO
-- INSERT [CTL].[BusinessRecConfig] ([BusinessRecConfigId], [BusinessReconGroup], [MeasureId], [MeasureName], [TargetObject], [TargetQuery], [Enabled]) VALUES (7, N'Water Consumption Reconciliation', N'9000', N'BILLING_DOC_COUNT', N'BilledWaterConsumption', N'select count(distinct fact.meterconsumptionbillingdocumentSK) as TargetMeasure from curated.factbilledwaterconsumption fact, curated.dimmeterconsumptionbillingdocument dimBillDoc, curated.dimmeter dimMeter where fact.sourceSystemCode = ''ISU'' and dimBillDoc.meterconsumptionbillingdocumentSK = fact.meterconsumptionbillingdocumentSK and fact.meterSK = dimmeter.meterSK and dimBillDoc.isOutsortedFlag = ''N'' and dimmeter.watertype in (''Drinking Water'') and dimmeter.usagemetertype in (''Customer Standpipe'') group by dimmeter.watertype, dimmeter.usagemetertype', 1)
-- GO
-- INSERT [CTL].[BusinessRecConfig] ([BusinessRecConfigId], [BusinessReconGroup], [MeasureId], [MeasureName], [TargetObject], [TargetQuery], [Enabled]) VALUES (8, N'Water Consumption Reconciliation', N'9000', N'CONSUMPTION', N'BilledWaterConsumption', N'select sum(fact.meteredWaterConsumption) as TargetMeasure from curated.factbilledwaterconsumption fact, curated.dimmeterconsumptionbillingdocument dimBillDoc, curated.dimmeter dimMeter where fact.sourceSystemCode = ''ISU'' and dimBillDoc.meterconsumptionbillingdocumentSK = fact.meterconsumptionbillingdocumentSK and fact.meterSK = dimmeter.meterSK and dimBillDoc.isOutsortedFlag = ''N'' and dimmeter.watertype in (''Drinking Water'') and dimmeter.usagemetertype in (''Customer Standpipe'') group by dimmeter.watertype, dimmeter.usagemetertype', 1)
-- GO
-- INSERT [CTL].[BusinessRecConfig] ([BusinessRecConfigId], [BusinessReconGroup], [MeasureId], [MeasureName], [TargetObject], [TargetQuery], [Enabled]) VALUES (9, N'Water Consumption Reconciliation', N'Total', N'BILLING_DOC_COUNT', N'BilledWaterConsumptionDaily', N'select count(distinct fact.meterconsumptionbillingdocumentSK) as TargetMeasure from curated.factdailyapportionedconsumption fact, curated.dimmeterconsumptionbillingdocument dim where fact.sourceSystemCode = ''ISU'' and dim.meterconsumptionbillingdocumentSK = fact.meterconsumptionbillingdocumentSK and dim.isOutsortedFlag = ''N''', 1)
-- GO
-- INSERT [CTL].[BusinessRecConfig] ([BusinessRecConfigId], [BusinessReconGroup], [MeasureId], [MeasureName], [TargetObject], [TargetQuery], [Enabled]) VALUES (10, N'Water Consumption Reconciliation', N'Total', N'CONSUMPTION', N'BilledWaterConsumptionDaily', N'select sum(fact.dailyApportionedConsumption) as TargetMeasure from curated.factdailyapportionedconsumption fact, curated.dimmeterconsumptionbillingdocument dim where fact.sourceSystemCode = ''ISU'' and dim.meterconsumptionbillingdocumentSK = fact.meterconsumptionbillingdocumentSK and dim.isOutsortedFlag = ''N''', 1)
-- GO
-- INSERT [CTL].[BusinessRecConfig] ([BusinessRecConfigId], [BusinessReconGroup], [MeasureId], [MeasureName], [TargetObject], [TargetQuery], [Enabled]) VALUES (11, N'Water Consumption Reconciliation', N'1000', N'BILLING_DOC_COUNT', N'BilledWaterConsumptionDaily', N'select count(distinct fact.meterconsumptionbillingdocumentSK) as TargetMeasure from curated.factdailyapportionedconsumption fact, curated.dimmeterconsumptionbillingdocument dimBillDoc, curated.dimmeter dimMeter where fact.sourceSystemCode = ''ISU'' and dimBillDoc.meterconsumptionbillingdocumentSK = fact.meterconsumptionbillingdocumentSK and fact.meterSK = dimmeter.meterSK and dimBillDoc.isOutsortedFlag = ''N'' and dimmeter.watertype in (''Drinking Water'') and dimmeter.usagemetertype in (''Water Meter'') group by dimmeter.watertype, dimmeter.usagemetertype', 1)
-- GO
-- INSERT [CTL].[BusinessRecConfig] ([BusinessRecConfigId], [BusinessReconGroup], [MeasureId], [MeasureName], [TargetObject], [TargetQuery], [Enabled]) VALUES (12, N'Water Consumption Reconciliation', N'1000', N'CONSUMPTION', N'BilledWaterConsumptionDaily', N'select sum(fact.dailyApportionedConsumption) as TargetMeasure from curated.factdailyapportionedconsumption fact, curated.dimmeterconsumptionbillingdocument dimBillDoc, curated.dimmeter dimMeter where fact.sourceSystemCode = ''ISU'' and dimBillDoc.meterconsumptionbillingdocumentSK = fact.meterconsumptionbillingdocumentSK and fact.meterSK = dimmeter.meterSK and dimBillDoc.isOutsortedFlag = ''N'' and dimmeter.watertype in (''Drinking Water'') and dimmeter.usagemetertype in (''Water Meter'') group by dimmeter.watertype, dimmeter.usagemetertype', 1)
-- GO
-- INSERT [CTL].[BusinessRecConfig] ([BusinessRecConfigId], [BusinessReconGroup], [MeasureId], [MeasureName], [TargetObject], [TargetQuery], [Enabled]) VALUES (13, N'Water Consumption Reconciliation', N'2000', N'CONSUMPTION', N'BilledWaterConsumptionDaily', N'select sum(fact.dailyApportionedConsumption) as TargetMeasure from curated.factdailyapportionedconsumption fact, curated.dimmeterconsumptionbillingdocument dimBillDoc, curated.dimmeter dimMeter where fact.sourceSystemCode = ''ISU'' and dimBillDoc.meterconsumptionbillingdocumentSK = fact.meterconsumptionbillingdocumentSK and fact.meterSK = dimmeter.meterSK and dimBillDoc.isOutsortedFlag = ''N'' and dimmeter.watertype in (''Recycled Water'') and dimmeter.usagemetertype in (''Water Meter'') group by dimmeter.watertype, dimmeter.usagemetertype', 1)
-- GO
-- INSERT [CTL].[BusinessRecConfig] ([BusinessRecConfigId], [BusinessReconGroup], [MeasureId], [MeasureName], [TargetObject], [TargetQuery], [Enabled]) VALUES (14, N'Water Consumption Reconciliation', N'2000', N'BILLING_DOC_COUNT', N'BilledWaterConsumptionDaily', N'select count(distinct fact.meterconsumptionbillingdocumentSK) as TargetMeasure from curated.factdailyapportionedconsumption fact, curated.dimmeterconsumptionbillingdocument dimBillDoc, curated.dimmeter dimMeter where fact.sourceSystemCode = ''ISU'' and dimBillDoc.meterconsumptionbillingdocumentSK = fact.meterconsumptionbillingdocumentSK and fact.meterSK = dimmeter.meterSK and dimBillDoc.isOutsortedFlag = ''N'' and dimmeter.watertype in (''Recycled Water'') and dimmeter.usagemetertype in (''Water Meter'') group by dimmeter.watertype, dimmeter.usagemetertype', 1)
-- GO
-- INSERT [CTL].[BusinessRecConfig] ([BusinessRecConfigId], [BusinessReconGroup], [MeasureId], [MeasureName], [TargetObject], [TargetQuery], [Enabled]) VALUES (15, N'Water Consumption Reconciliation', N'9000', N'BILLING_DOC_COUNT', N'BilledWaterConsumptionDaily', N'select count(distinct fact.meterconsumptionbillingdocumentSK) as TargetMeasure from curated.factdailyapportionedconsumption fact, curated.dimmeterconsumptionbillingdocument dimBillDoc, curated.dimmeter dimMeter where fact.sourceSystemCode = ''ISU'' and dimBillDoc.meterconsumptionbillingdocumentSK = fact.meterconsumptionbillingdocumentSK and fact.meterSK = dimmeter.meterSK and dimBillDoc.isOutsortedFlag = ''N'' and dimmeter.watertype in (''Drinking Water'') and dimmeter.usagemetertype in (''Customer Standpipe'') group by dimmeter.watertype, dimmeter.usagemetertype', 1)
-- GO
-- INSERT [CTL].[BusinessRecConfig] ([BusinessRecConfigId], [BusinessReconGroup], [MeasureId], [MeasureName], [TargetObject], [TargetQuery], [Enabled]) VALUES (16, N'Water Consumption Reconciliation', N'9000', N'CONSUMPTION', N'BilledWaterConsumptionDaily', N'select sum(fact.dailyApportionedConsumption) as TargetMeasure from curated.factdailyapportionedconsumption fact, curated.dimmeterconsumptionbillingdocument dimBillDoc, curated.dimmeter dimMeter where fact.sourceSystemCode = ''ISU'' and dimBillDoc.meterconsumptionbillingdocumentSK = fact.meterconsumptionbillingdocumentSK and fact.meterSK = dimmeter.meterSK and dimBillDoc.isOutsortedFlag = ''N'' and dimmeter.watertype in (''Drinking Water'') and dimmeter.usagemetertype in (''Customer Standpipe'') group by dimmeter.watertype, dimmeter.usagemetertype', 1)
-- GO
-- SET IDENTITY_INSERT [CTL].[BusinessRecConfig] OFF
-- GO

-- DELETE FROM CTL.TechRecCleansedConfig;
-- DBCC CHECKIDENT ('CTL.TechRecCleansedConfig',Reseed,0)

-- Insert into CTL.TechRecCleansedConfig values ('isu_0UCCONTRACTH_ATTR_2','WHERE _recordcurrent = 1 and _recorddeleted = 0','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0UC_DEVINST_ATTR','WHERE _recordcurrent = 1 and _recorddeleted = 0','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0UC_DEVICEH_ATTR','WHERE _recordcurrent = 1 and _recorddeleted = 0','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_ERCH','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_DBERCHZ1','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_DBERCHZ2','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_DBERCHZ3','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0BP_RELATIONS_ATTR','WHERE relationshipDirection = 2 AND deletedIndicator IS NULL','N');
-- Insert into CTL.TechRecCleansedConfig values ('crm_0BPARTNER_ATTR','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0BPARTNER_ATTR','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0CACONT_ACC_ATTR_2','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0UC_CONNOBJ_ATTR_2','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0UC_DEVCAT_ATTR','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0UC_DEVICE_ATTR','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0UCCONTRACT_ATTR_2','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0UCINSTALLA_ATTR_2','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_DD07T','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_VIBDNODE','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_VIBDAO','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_ERCHC','','N');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0UCINSTALLAH_ATTR_2','WHERE _recordcurrent = 1 and _recorddeleted = 0','Y');
-- Insert into CTL.TechRecCleansedConfig values ('crm_0BP_ID_ATTR','WHERE _recordcurrent = 1 and _recorddeleted = 0','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0BP_ID_ATTR','WHERE _recordcurrent = 1 and _recorddeleted = 0','N');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0UC_REGINST_STR_ATTR','WHERE _recordcurrent = 1 and _recorddeleted = 0','N');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0UC_REGIST_ATTR','WHERE _recordcurrent = 1 and _recorddeleted = 0','N');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0UC_MTR_DOC','WHERE _recordcurrent = 1 and _recorddeleted = 0','N');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0UC_MTR_DOCR','WHERE _recordcurrent = 1 and _recorddeleted = 0','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0COMP_CODE_TEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0DIVISION_TEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0UCMTRDUNIT_ATTR','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_ADDRC_DELI_SERVT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0FCACTDETID_TEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0UC_ABRSPERR_TEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0UC_STATTART_TEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_T005T','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_TE405T','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_TSAD3T','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_TE835T','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0UC_AKLASSE_TEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0UC_TARIFTYP_TEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0UC_TARIFNR_TEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_CAWN','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_TE128T','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_TE438','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_TE420','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0UC_ZAHLKOND_TEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('crm_TSAD3T','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0UC_ISU_32','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('crm_0SVY_QUEST_TEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('crm_DD07T','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('crm_0CAM_STREETCODE_TEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('crm_0SVY_QSTNNR_TEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('crm_0SVY_ANSWER_TEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0UCPREMISE_ATTR_2','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0UC_ACCNTBP_ATTR_2','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0PM_MEASPOINT_ATTR','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_ZCD_TPLANTYPE_TX','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_ZCD_VIRELTYPTX','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_ZCD_TINFPRTY_TX','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_ZCD_TSUPPRTYP_TX','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_ZCD_VIRELTYP2TX','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_ZCD_TPROCTYPE_TX','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_ICADOCORIGCODET','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_TE609T','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_TE227T','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_ICACLEARINGRSNT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0UC_PROFILE_TEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0UC_REGGRP_TEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0UC_PRICCLA_TEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0UCDIVISCAT_TEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0GHO_NETOBJS_TEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0UC_DISCREAS_TEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0PM_MEASPOINT_TEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0UC_BBPROC_TEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0UC_ABRVORG_TEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0UC_APPLK_TEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0UC_ICHCKCD_TEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0ARCHOBJECT_TEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0BP_CAT_TEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0CAM_STREETCODE_TEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0FC_PPSTA_TEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_ZCD_TPROPTY_HIST','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_ZCD_TPROP_REL','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_TIVBDCHARACTT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_TIVBDAROBJTYPET','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_TFK047XT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_TFK043T','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_TE523T','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_TE438T','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_TE192T','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_TE065T','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_IFORMOFADDRTEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_ICAWRITEOFFRSNT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_ERCHO','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_EHAUISU','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_EFRM','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_EDISCORDSTATET','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_EASTIH','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_CAWNT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_CABN','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0UC_VKTYP_TEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0UC_TVORG_TEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0UC_SERTYPE_TEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0UC_PROLE_TEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0UC_PORTION_TEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0UC_OPERAND_TEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0UC_MRCATEG_TEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0UC_IND_SEC_TEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0UC_HVORG_TEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0UC_GERWECHS_TEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0UC_FUNKLAS_TEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0UC_BAUKLAS_TEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0IND_SECTOR_TEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0IND_NUMSYS_TEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0FL_TYPE_TEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0FC_STEP_TEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0FC_PYMET_TEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0FC_PPRSW_TEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0FC_PPRS_TEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0FC_PPCAT_TEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0FC_BLART_TEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0FC_ACCTREL_TEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0DF_REFIXFI_TEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0BPTYPE_TEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0BP_ID_TYPE_TEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0BP_GROUP_TEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('crm_0BPTYPE_TEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('crm_0BP_ID_TYPE_TEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('crm_0BP_GROUP_TEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('crm_0BP_CAT_TEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_SCAL_TT_DATE','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0UC_DISCPRV_TEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_ESENDCONTROLT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0UC_MRTYPE_TEXT','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('isu_0FC_DUN_HEADER','','Y');
-- Insert into CTL.TechRecCleansedConfig values ('crm_CRMC_BUAG_PAYM_T','','Y');

-- GO
