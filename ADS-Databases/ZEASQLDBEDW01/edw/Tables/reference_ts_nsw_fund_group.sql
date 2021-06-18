CREATE TABLE [edw].[reference_ts_nsw_fund_group] (
    [TSNSWFundGroupCode]        NVARCHAR (4)                             NULL,
    [TSNSWFundGroupName]        NVARCHAR (60)                            NULL,
    [TSNSWFundGroupDescription] NVARCHAR (65)                            NULL,
    [FundingSourceNationalID]   INT MASKED WITH (FUNCTION = 'default()') NULL,
    [_DLRawZoneTimeStamp]       DATETIME2 (7)                            NULL,
    [_DLTrustedZoneTimeStamp]   DATETIME2 (7)                            NULL,
    [_RecordStart]              DATETIME2 (7)                            NULL,
    [_RecordEnd]                DATETIME2 (7)                            NULL,
    [_RecordCurrent]            INT                                      NULL,
    [_RecordDeleted]            INT                                      NULL
);





