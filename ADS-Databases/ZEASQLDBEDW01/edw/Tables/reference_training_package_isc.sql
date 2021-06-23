CREATE TABLE [edw].[reference_training_package_isc] (
    [TrainingPackageSubstring] NVARCHAR (4)  NULL,
    [IndustrySkillsCouncilID]  INT           NULL,
    [_DLRawZoneTimeStamp]      DATETIME2 (7) NULL,
    [_DLTrustedZoneTimeStamp]  DATETIME2 (7) NULL,
    [_RecordStart]             DATETIME2 (7) NULL,
    [_RecordEnd]               DATETIME2 (7) NULL,
    [_RecordCurrent]           INT           NULL,
    [_RecordDeleted]           INT           NULL
);



