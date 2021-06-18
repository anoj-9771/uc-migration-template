CREATE TABLE [edw].[reference_qualification_type_mapping] (
    [QualificationTypeID]     INT           NULL,
    [QualificationGroupID]    INT           NULL,
    [AQFLevelID]              INT           NULL,
    [AQFGroupID]              INT           NULL,
    [BPRLevelID]              INT           NULL,
    [Order]                   INT           NULL,
    [BPRQualifcationGroupID]  INT           NULL,
    [_DLRawZoneTimeStamp]     DATETIME2 (7) NULL,
    [_DLTrustedZoneTimeStamp] DATETIME2 (7) NULL,
    [_RecordStart]            DATETIME2 (7) NULL,
    [_RecordEnd]              DATETIME2 (7) NULL,
    [_RecordCurrent]          INT           NULL,
    [_RecordDeleted]          INT           NULL
);



