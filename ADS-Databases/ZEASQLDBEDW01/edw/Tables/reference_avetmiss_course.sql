CREATE TABLE [edw].[reference_avetmiss_course] (
    [CourseCodeConverted]     NVARCHAR (22) NULL,
    [AvetmissCourseCode]      NVARCHAR (10) NULL,
    [_DLRawZoneTimeStamp]     DATETIME2 (7) NULL,
    [_DLTrustedZoneTimeStamp] DATETIME2 (7) NULL,
    [_RecordStart]            DATETIME2 (7) NULL,
    [_RecordEnd]              DATETIME2 (7) NULL,
    [_RecordCurrent]          INT           NULL,
    [_RecordDeleted]          INT           NULL
);



