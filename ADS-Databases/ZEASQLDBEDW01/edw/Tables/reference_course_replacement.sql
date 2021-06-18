CREATE TABLE [edw].[reference_course_replacement] (
    [NationalCourseCode]            NVARCHAR (25) NULL,
    [ReplacementNationalCourseCode] NVARCHAR (25) NULL,
    [_DLRawZoneTimeStamp]           DATETIME2 (7) NULL,
    [_DLTrustedZoneTimeStamp]       DATETIME2 (7) NULL,
    [_RecordStart]                  DATETIME2 (7) NULL,
    [_RecordEnd]                    DATETIME2 (7) NULL,
    [_RecordCurrent]                INT           NULL,
    [_RecordDeleted]                INT           NULL
);



