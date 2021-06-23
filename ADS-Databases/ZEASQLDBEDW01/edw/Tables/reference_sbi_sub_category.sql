CREATE TABLE [edw].[reference_sbi_sub_category] (
    [SBISubCategoryID]        INT           NULL,
    [SBISubCategory]          NVARCHAR (80) NULL,
    [SBICategoryID]           INT           NULL,
    [_DLRawZoneTimeStamp]     DATETIME2 (7) NULL,
    [_DLTrustedZoneTimeStamp] DATETIME2 (7) NULL,
    [_RecordStart]            DATETIME2 (7) NULL,
    [_RecordEnd]              DATETIME2 (7) NULL,
    [_RecordCurrent]          INT           NULL,
    [_RecordDeleted]          INT           NULL
);



