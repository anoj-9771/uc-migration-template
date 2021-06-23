CREATE TABLE [edw].[reference_nat_collection_dates] (
    [ReportingYear]           NVARCHAR (6)  NULL,
    [Quarter]                 INT           NULL,
    [CollectionStart]         DATE          NULL,
    [AvetmissCollectionEnd]   DATE          NULL,
    [EreportingCollectionEnd] DATE          NULL,
    [_DLRawZoneTimeStamp]     DATETIME2 (7) NULL,
    [_DLTrustedZoneTimeStamp] DATETIME2 (7) NULL,
    [_RecordStart]            DATETIME2 (7) NULL,
    [_RecordEnd]              DATETIME2 (7) NULL,
    [_RecordCurrent]          INT           NULL,
    [_RecordDeleted]          INT           NULL
);



