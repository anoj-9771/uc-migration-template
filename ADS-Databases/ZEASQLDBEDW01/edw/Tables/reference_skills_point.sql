CREATE TABLE [edw].[reference_skills_point] (
    [SkillsPointID]           INT           NULL,
    [SkillsPointCode]         NVARCHAR (10) NULL,
    [SkillsPointName]         NVARCHAR (70) NULL,
    [SkillsPointDescription]  NVARCHAR (73) NULL,
    [_DLRawZoneTimeStamp]     DATETIME2 (7) NULL,
    [_DLTrustedZoneTimeStamp] DATETIME2 (7) NULL,
    [_RecordStart]            DATETIME2 (7) NULL,
    [_RecordEnd]              DATETIME2 (7) NULL,
    [_RecordCurrent]          INT           NULL,
    [_RecordDeleted]          INT           NULL
);



