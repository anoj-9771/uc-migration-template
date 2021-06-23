CREATE TABLE [compliance].[AvetmissDisabilityType] (
    [AvetmissDisabilityTypeSK] BIGINT        NULL,
    [PersonCode]               BIGINT        NULL,
    [ReportingYear]            INT           NULL,
    [DisabilityType]           NVARCHAR (50) NULL,
    [_DLCuratedZoneTimeStamp]  DATETIME2 (7) NULL,
    [_RecordStart]             DATETIME2 (7) NULL,
    [_RecordEnd]               DATETIME2 (7) NULL,
    [_RecordDeleted]           INT           NULL,
    [_RecordCurrent]           INT           NULL
);






GO


