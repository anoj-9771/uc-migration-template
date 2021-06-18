CREATE TABLE [edw].[reference_ts_nsw_fund] (
    [TSNSWFundCode]           NVARCHAR (3)  NULL,
    [TSNSWFundName]           NVARCHAR (60) NULL,
    [TSNSWFundDescription]    NVARCHAR (65) NULL,
    [FundingSourceCode]       NVARCHAR (5)  NULL,
    [_DLRawZoneTimeStamp]     DATETIME2 (7) NULL,
    [_DLTrustedZoneTimeStamp] DATETIME2 (7) NULL,
    [_RecordStart]            DATETIME2 (7) NULL,
    [_RecordEnd]              DATETIME2 (7) NULL,
    [_RecordCurrent]          INT           NULL,
    [_RecordDeleted]          INT           NULL
);



