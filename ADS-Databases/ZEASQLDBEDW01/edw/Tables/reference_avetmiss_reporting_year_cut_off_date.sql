CREATE TABLE [edw].[reference_avetmiss_reporting_year_cut_off_date] (
    [ReportingYear]               NVARCHAR (6)  NULL,
    [CubeCutOffReportingDate]     DATE          NULL,
    [AvetmissCutOffReportingDate] DATE          NULL,
    [_DLRawZoneTimeStamp]         DATETIME2 (7) NULL,
    [_DLTrustedZoneTimeStamp]     DATETIME2 (7) NULL,
    [_RecordStart]                DATETIME2 (7) NULL,
    [_RecordEnd]                  DATETIME2 (7) NULL,
    [_RecordCurrent]              INT           NULL,
    [_RecordDeleted]              INT           NULL
);



