CREATE TABLE [edw].[reference_avetmiss_minimum_units] (
    [AvetmissCourseCode]      NVARCHAR (10) NULL,
    [MinimumRequiredUOCS]     INT           NULL,
    [MinimumRequiredCoreUOCS] INT           NULL,
    [_DLRawZoneTimeStamp]     DATETIME2 (7) NULL,
    [_DLTrustedZoneTimeStamp] DATETIME2 (7) NULL,
    [_RecordStart]            DATETIME2 (7) NULL,
    [_RecordEnd]              DATETIME2 (7) NULL,
    [_RecordCurrent]          INT           NULL,
    [_RecordDeleted]          INT           NULL
);



