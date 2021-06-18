CREATE TABLE [edw].[reference_sbi_category] (
    [SBICategoryID]           INT           NULL,
    [SBICategoryName]         NVARCHAR (45) NULL,
    [SBIGroupID]              INT           NULL,
    [_DLRawZoneTimeStamp]     DATETIME2 (7) NULL,
    [_DLTrustedZoneTimeStamp] DATETIME2 (7) NULL,
    [_RecordStart]            DATETIME2 (7) NULL,
    [_RecordEnd]              DATETIME2 (7) NULL,
    [_RecordCurrent]          INT           NULL,
    [_RecordDeleted]          INT           NULL
);



