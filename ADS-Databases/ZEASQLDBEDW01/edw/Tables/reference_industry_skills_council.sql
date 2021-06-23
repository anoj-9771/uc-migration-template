CREATE TABLE [edw].[reference_industry_skills_council] (
    [IndustrySkillsCouncilID]   INT           NULL,
    [IndustrySkillsCouncilName] NVARCHAR (45) NULL,
    [_DLRawZoneTimeStamp]       DATETIME2 (7) NULL,
    [_DLTrustedZoneTimeStamp]   DATETIME2 (7) NULL,
    [_RecordStart]              DATETIME2 (7) NULL,
    [_RecordEnd]                DATETIME2 (7) NULL,
    [_RecordCurrent]            INT           NULL,
    [_RecordDeleted]            INT           NULL
);



