CREATE TABLE [edw].[reference_stratification_group] (
    [Stratification]          NVARCHAR (30) NULL,
    [Group]                   NVARCHAR (30) NULL,
    [LowValue]                INT           NULL,
    [HighValue]               INT           NULL,
    [_DLRawZoneTimeStamp]     DATETIME2 (7) NULL,
    [_DLTrustedZoneTimeStamp] DATETIME2 (7) NULL,
    [_RecordStart]            DATETIME2 (7) NULL,
    [_RecordEnd]              DATETIME2 (7) NULL,
    [_RecordCurrent]          INT           NULL,
    [_RecordDeleted]          INT           NULL
);



