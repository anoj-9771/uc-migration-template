CREATE TABLE [edw].[eChecklist_dbo_checklists_task_statuses] (
    [TaskStatusId]            INT           NULL,
    [TaskStatusName]          VARCHAR (50)  NULL,
    [_DLRawZoneTimeStamp]     DATETIME2 (7) NULL,
    [_DLTrustedZoneTimeStamp] DATETIME2 (7) NULL,
    [_RecordStart]            DATETIME2 (7) NULL,
    [_RecordEnd]              DATETIME2 (7) NULL,
    [_RecordCurrent]          INT           NULL,
    [_RecordDeleted]          INT           NULL
);



