CREATE TABLE [edw].[reference_specific_funding] (
    [SpecificFundingCodeID]   INT           NULL,
    [SpecificFundingName]     NVARCHAR (45) NULL,
    [_DLRawZoneTimeStamp]     DATETIME2 (7) NULL,
    [_DLTrustedZoneTimeStamp] DATETIME2 (7) NULL,
    [_RecordStart]            DATETIME2 (7) NULL,
    [_RecordEnd]              DATETIME2 (7) NULL,
    [_RecordCurrent]          INT           NULL,
    [_RecordDeleted]          INT           NULL
);



