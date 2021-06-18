CREATE TABLE [edw].[eChecklist_dbo_checklists_instance_history] (
    [InstanceHistoryId]       INT            NULL,
    [InstanceId]              INT            NULL,
    [ActionedDate]            SMALLDATETIME  NULL,
    [Owner]                   VARCHAR (20)   NULL,
    [Action]                  VARCHAR (500)  NULL,
    [TaskNo]                  INT            NULL,
    [Comment]                 VARCHAR (8000) NULL,
    [_DLRawZoneTimeStamp]     DATETIME2 (7)  NULL,
    [_DLTrustedZoneTimeStamp] DATETIME2 (7)  NULL,
    [_RecordStart]            DATETIME2 (7)  NULL,
    [_RecordEnd]              DATETIME2 (7)  NULL,
    [_RecordCurrent]          INT            NULL,
    [_RecordDeleted]          INT            NULL
);





