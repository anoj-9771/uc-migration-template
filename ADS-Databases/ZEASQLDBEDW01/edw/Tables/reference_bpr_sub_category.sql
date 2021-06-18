CREATE TABLE [edw].[reference_bpr_sub_category] (
    [BPRSubCategoryID]          NVARCHAR (3)  NULL,
    [BPRSubCategoryName]        NVARCHAR (60) NULL,
    [BPRSubCategoryDescription] NVARCHAR (65) NULL,
    [BPRCategoryID]             INT           NULL,
    [_DLRawZoneTimeStamp]       DATETIME2 (7) NULL,
    [_DLTrustedZoneTimeStamp]   DATETIME2 (7) NULL,
    [_RecordStart]              DATETIME2 (7) NULL,
    [_RecordEnd]                DATETIME2 (7) NULL,
    [_RecordCurrent]            INT           NULL,
    [_RecordDeleted]            INT           NULL
);



