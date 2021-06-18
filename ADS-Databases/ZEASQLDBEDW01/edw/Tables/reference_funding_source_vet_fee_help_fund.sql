CREATE TABLE [edw].[reference_funding_source_vet_fee_help_fund] (
    [FundingSourceCode]       NVARCHAR (5)  NULL,
    [VETFeeHelpFundID]        INT           NULL,
    [_DLRawZoneTimeStamp]     DATETIME2 (7) NULL,
    [_DLTrustedZoneTimeStamp] DATETIME2 (7) NULL,
    [_RecordStart]            DATETIME2 (7) NULL,
    [_RecordEnd]              DATETIME2 (7) NULL,
    [_RecordCurrent]          INT           NULL,
    [_RecordDeleted]          INT           NULL
);



