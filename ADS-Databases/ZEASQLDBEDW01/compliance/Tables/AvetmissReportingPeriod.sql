CREATE TABLE [compliance].[AvetmissReportingPeriod] (
    [AvetmissReportingPeriodSK]  BIGINT        NULL,
    [ReportingPeriodKey]         INT           NULL,
    [ReportingDate]              DATE          NULL,
    [ReportingWeek]              INT           NULL,
    [ReportingMonthNo]           INT           NULL,
    [ReportingMonthName]         NVARCHAR (11) NULL,
    [ReportingQuarter]           INT           NULL,
    [ReportingSemester]          INT           NULL,
    [ReportingYear]              NVARCHAR (7)  NULL,
    [ReportingDatePreviousYear]  DATE          NULL,
    [ReportingDateNextYear]      DATE          NULL,
    [ReportingYearNo]            INT           NULL,
    [MonthNameFY]                NVARCHAR (14) NULL,
    [SpecialReportingYear]       INT           NULL,
    [ReportingPeriodKeyLastYear] INT           NULL,
    [ReportingYearStartDate]     DATE          NULL,
    [ReportingYearEndDate]       DATE          NULL,
    [_DLCuratedZoneTimeStamp]    DATETIME2 (7) NULL,
    [_RecordStart]               DATETIME2 (7) NULL,
    [_RecordEnd]                 DATETIME2 (7) NULL,
    [_RecordDeleted]             INT           NULL,
    [_RecordCurrent]             INT           NULL
);




GO
CREATE NONCLUSTERED INDEX [IX_Compliance_AvetmissReportingPeriod_ReportingYear_ReportingDate]
    ON [compliance].[AvetmissReportingPeriod]([ReportingYear] ASC, [ReportingDate] ASC);


GO
CREATE CLUSTERED COLUMNSTORE INDEX [CCI_Compliance_AvetmissReportingPeriod] ON [compliance].[AvetmissReportingPeriod]
WITH (DROP_EXISTING = OFF, COMPRESSION_DELAY = 0) 
;

