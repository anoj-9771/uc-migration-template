CREATE TABLE [edw].[reference_qualification_group] (
    [QualificationGroupID]          INT           NULL,
    [QualificationGroupName]        NVARCHAR (40) NULL,
    [QualificationGroupDescription] NVARCHAR (45) NULL,
    [QualificationGroupShort]       NVARCHAR (35) NULL,
    [_DLRawZoneTimeStamp]           DATETIME2 (7) NULL,
    [_DLTrustedZoneTimeStamp]       DATETIME2 (7) NULL,
    [_RecordStart]                  DATETIME2 (7) NULL,
    [_RecordEnd]                    DATETIME2 (7) NULL,
    [_RecordCurrent]                INT           NULL,
    [_RecordDeleted]                INT           NULL
);



