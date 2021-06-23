CREATE TABLE [edw].[reference_bpr_qualification_group] (
    [BPRQualificationGroupID]          INT           NULL,
    [BPRQualificationGroupName]        NVARCHAR (27) NULL,
    [BPRQualificationGroupDescription] NVARCHAR (32) NULL,
    [_DLRawZoneTimeStamp]              DATETIME2 (7) NULL,
    [_DLTrustedZoneTimeStamp]          DATETIME2 (7) NULL,
    [_RecordStart]                     DATETIME2 (7) NULL,
    [_RecordEnd]                       DATETIME2 (7) NULL,
    [_RecordCurrent]                   INT           NULL,
    [_RecordDeleted]                   INT           NULL
);



