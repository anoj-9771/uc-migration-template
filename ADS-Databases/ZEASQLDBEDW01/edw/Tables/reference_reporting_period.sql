CREATE TABLE [edw].[reference_reporting_period] (
    [ReportingPeriodKey]         INT           NULL,
    [ReportingDate]              DATE          NULL,
    [ReportingWeek]              INT           NULL,
    [ReportingMonthNo]           INT           NULL,
    [ReportingMonthName]         NVARCHAR (11) NULL,
    [ReportingQuarter]           INT           NULL,
    [ReportingSemester]          INT           NULL,
    [ReportingYear]              NVARCHAR (6)  NULL,
    [ReportingDatePreviousYear]  DATE          NULL,
    [ReportingDateNextYear]      DATE          NULL,
    [ReportingYearNo]            INT           NULL,
    [MonthNameFY]                NVARCHAR (14) NULL,
    [SpecialReportingYear]       INT           NULL,
    [ReportingPeriodKeyLastYear] INT           NULL,
    [ReportingYearStartDate]     DATE          NULL,
    [ReportingYearEndDate]       DATE          NULL,
    [_DLRawZoneTimeStamp]        DATETIME2 (7) NULL,
    [_DLTrustedZoneTimeStamp]    DATETIME2 (7) NULL,
    [_RecordStart]               DATETIME2 (7) NULL,
    [_RecordEnd]                 DATETIME2 (7) NULL,
    [_RecordCurrent]             INT           NULL,
    [_RecordDeleted]             INT           NULL
);



