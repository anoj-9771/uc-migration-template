CREATE TABLE [edw].[reference_semester] (
    [SemesterID]              INT           NULL,
    [SemesterNumber]          INT           NULL,
    [SemesterYear]            INT           NULL,
    [SemesterName]            NVARCHAR (18) NULL,
    [SemesterDescription]     NVARCHAR (18) NULL,
    [SemesterStartDate]       DATE          NULL,
    [SemesterEndDate]         DATE          NULL,
    [_DLRawZoneTimeStamp]     DATETIME2 (7) NULL,
    [_DLTrustedZoneTimeStamp] DATETIME2 (7) NULL,
    [_RecordStart]            DATETIME2 (7) NULL,
    [_RecordEnd]              DATETIME2 (7) NULL,
    [_RecordCurrent]          INT           NULL,
    [_RecordDeleted]          INT           NULL
);



