CREATE TABLE [edw].[eChecklist_dbo_checklists_work_unit_locales] (
    [WorkUnitLocaleId]        SMALLINT      NULL,
    [WorkUnitLocaleName]      VARCHAR (100) NULL,
    [WULPublicName]           VARCHAR (100) NULL,
    [Active]                  BIT           NULL,
    [_DLRawZoneTimeStamp]     DATETIME2 (7) NULL,
    [_DLTrustedZoneTimeStamp] DATETIME2 (7) NULL,
    [_RecordStart]            DATETIME2 (7) NULL,
    [_RecordEnd]              DATETIME2 (7) NULL,
    [_RecordCurrent]          INT           NULL,
    [_RecordDeleted]          INT           NULL
);



