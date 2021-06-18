CREATE TABLE [edw].[reference_funding_source_bpr] (
    [FundingSourceCode]       NVARCHAR (5)  NULL,
    [BPRSubCategoryID]        NVARCHAR (3)  NULL,
    [_DLRawZoneTimeStamp]     DATETIME2 (7) NULL,
    [_DLTrustedZoneTimeStamp] DATETIME2 (7) NULL,
    [_RecordStart]            DATETIME2 (7) NULL,
    [_RecordEnd]              DATETIME2 (7) NULL,
    [_RecordCurrent]          INT           NULL,
    [_RecordDeleted]          INT           NULL
);



