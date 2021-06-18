CREATE TABLE [edw].[reference_lga] (
    [Suburb]                  NVARCHAR (60) NULL,
    [Postcode]                NVARCHAR (5)  NULL,
    [AusState]                NVARCHAR (3)  NULL,
    [LGACode2011]             INT           NULL,
    [LGAName2011]             NVARCHAR (40) NULL,
    [_DLRawZoneTimeStamp]     DATETIME2 (7) NULL,
    [_DLTrustedZoneTimeStamp] DATETIME2 (7) NULL,
    [_RecordStart]            DATETIME2 (7) NULL,
    [_RecordEnd]              DATETIME2 (7) NULL,
    [_RecordCurrent]          INT           NULL,
    [_RecordDeleted]          INT           NULL
);





