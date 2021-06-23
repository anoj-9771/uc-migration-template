CREATE TABLE [edw].[reference_avetmiss_course_skills_point] (
    [AvetmissCourseCode]      NVARCHAR (22) NULL,
    [SkillsPointID]           INT           NULL,
    [_DLRawZoneTimeStamp]     DATETIME2 (7) NULL,
    [_DLTrustedZoneTimeStamp] DATETIME2 (7) NULL,
    [_RecordStart]            DATETIME2 (7) NULL,
    [_RecordEnd]              DATETIME2 (7) NULL,
    [_RecordCurrent]          INT           NULL,
    [_RecordDeleted]          INT           NULL
);



