CREATE TABLE [edw].[eChecklist_dbo_checklists_referential] (
    [Domain]                  VARCHAR (100)  NULL,
    [Value]                   VARCHAR (20)   NULL,
    [ShortName]               VARCHAR (20)   NULL,
    [LongName]                VARCHAR (1000) NULL,
    [Active]                  BIT            NULL,
    [Misc]                    VARCHAR (4000) NULL,
    [DisplayOrder]            INT            NULL,
    [FKey1]                   INT            NULL,
    [FKey2]                   VARCHAR (100)  NULL,
    [_DLRawZoneTimeStamp]     DATETIME2 (7)  NULL,
    [_DLTrustedZoneTimeStamp] DATETIME2 (7)  NULL,
    [_RecordStart]            DATETIME2 (7)  NULL,
    [_RecordEnd]              DATETIME2 (7)  NULL,
    [_RecordCurrent]          INT            NULL,
    [_RecordDeleted]          INT            NULL
);



