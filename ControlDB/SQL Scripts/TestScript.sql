SELECT *  FROM [CTL].[ControlProjects]
--Truncate table [CTL].[ControlProjects]
SELECT *  FROM [CTL].[ControlProjectSchedule]
--Truncate table [CTL].[ControlProjectSchedule]
SELECT *  FROM [CTL].[ControlStages]
SELECT *  FROM [CTL].[ControlTypes]
SELECT *  FROM [CTL].[ControlDataLoadTypes]

SELECT *  FROM [CTL].[ControlTarget]
SELECT *  FROM [CTL].[ControlSource]

SELECT *  FROM [CTL].[ControlTasks]
SELECT *  FROM [CTL].[ControlTaskCommand]

SELECT *  FROM [CTL].[TaskExecutionLog]
--truncate table [CTL].[TaskExecutionLog]
--DELETE FROM CTL.TaskExecutionLog WHERE ExecutionLogId = 20

SELECT * FROM ctl.TaskExecutionLog WHERE ExecutionStatus = 'In Progress'
SELECT * FROM ctl.BatchExecutionLog WHERE BatchExecutionStatus = 'In Progress'
--EXEC [CTL].[ClearErrorLogInProgress]
--

select * from [CTL].[ControlManifest]
select * from [CTL].[BatchExecutionLog]
--DELETE FROM [CTL].[BatchExecutionLog] WHERE BatchExecutionLogId = 2
--update [CTL].[BatchExecutionLog] set BatchExecutionStatus = 'Complete' WHERE BatchExecutionLogId = 4 EDDIS_TICK_TOCK

QLIK/EDDIS/EDDIS_OWNER.ADDRESSCONTACT
QLIK/EDDIS/CDC_ATTUNITY.EDDIS_TICK_TOCK

--update [CTL].[ControlManifest]
--set FileName = 'test'
--where BatchExecutionLogID = 36
BLOB Storage (json).dfm



--IF (EXISTS (SELECT * 
--                 FROM INFORMATION_SCHEMA.TABLES 
--                 WHERE TABLE_SCHEMA = 'QLIK_ODSEDDIS' 
--                 AND  TABLE_NAME = 'MNEWHAM_DEPT'))
--BEGIN
--    TRUNCATE TABLE QLIK_ODSEDDIS.MNEWHAM_DEPT;
--END;

--select 1 from ctl.[ControlManifest]
--where FolderName = 'ADS/QLIK/EDDiS/EDDIS_OWNER.CHANNEL' and
--      FileName = 'LOAD00000001.json.gz'

