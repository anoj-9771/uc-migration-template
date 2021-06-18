CREATE TABLE [edw].[reference_institute] (
    [InstituteID]             INT           NULL,
    [InstituteCode]           NVARCHAR (5)  NULL,
    [RTOCode]                 INT           NULL,
    [_DLRawZoneTimeStamp]     DATETIME2 (7) NULL,
    [_DLTrustedZoneTimeStamp] DATETIME2 (7) NULL,
    [_RecordStart]            DATETIME2 (7) NULL,
    [_RecordEnd]              DATETIME2 (7) NULL,
    [_RecordCurrent]          INT           NULL,
    [_RecordDeleted]          INT           NULL
);



