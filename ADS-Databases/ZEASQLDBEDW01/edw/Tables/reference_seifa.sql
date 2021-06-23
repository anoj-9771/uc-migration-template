CREATE TABLE [edw].[reference_seifa] (
    [Suburb]                  NVARCHAR (70) NULL,
    [Postcode]                NVARCHAR (5)  NULL,
    [AusState]                NVARCHAR (3)  NULL,
    [SA2Code]                 INT           NULL,
    [SA2Name]                 NVARCHAR (40) NULL,
    [SA4Code]                 INT           NULL,
    [SA4Name]                 NVARCHAR (70) NULL,
    [SSRegion]                NVARCHAR (35) NULL,
    [ARIA11]                  NVARCHAR (30) NULL,
    [ARIA11B]                 NVARCHAR (15) NULL,
    [Population]              INT           NULL,
    [Score]                   INT           NULL,
    [Rank]                    INT           NULL,
    [Decile]                  INT           NULL,
    [Percentile]              INT           NULL,
    [_DLRawZoneTimeStamp]     DATETIME2 (7) NULL,
    [_DLTrustedZoneTimeStamp] DATETIME2 (7) NULL,
    [_RecordStart]            DATETIME2 (7) NULL,
    [_RecordEnd]              DATETIME2 (7) NULL,
    [_RecordCurrent]          INT           NULL,
    [_RecordDeleted]          INT           NULL
);





