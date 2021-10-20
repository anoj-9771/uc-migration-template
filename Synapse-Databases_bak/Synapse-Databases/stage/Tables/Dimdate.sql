CREATE TABLE [stage].[Dimdate] (
    [DimDateSK]               BIGINT         NOT NULL,
    [calendarDate]            DATE           NOT NULL,
    [dayOfWeek]               INT            NOT NULL,
    [dayName]                 NVARCHAR (256) NOT NULL,
    [dayOfMonth]              INT            NOT NULL,
    [dayOfYear]               INT            NOT NULL,
    [monthOfYear]             INT            NOT NULL,
    [monthName]               NVARCHAR (256) NOT NULL,
    [quarterOfYear]           INT            NOT NULL,
    [halfOfYear]              INT            NOT NULL,
    [monthStartDate]          DATE           NOT NULL,
    [monthEndDate]            DATE           NOT NULL,
    [yearStartDate]           DATE           NOT NULL,
    [yearEndDate]             DATE           NOT NULL,
    [financialYear]           NVARCHAR (256) NOT NULL,
    [financialYearStartDate]  DATE           NOT NULL,
    [financialYearEndDate]    DATE           NULL,
    [monthOfFinancialYear]    INT            NOT NULL,
    [quarterOfFinancialYear]  INT            NOT NULL,
    [halfOfFinancialYear]     INT            NOT NULL,
    [_DLCuratedZoneTimeStamp] DATETIME2 (7)  NOT NULL,
    [_RecordStart]            DATETIME2 (7)  NOT NULL,
    [_RecordEnd]              DATETIME2 (7)  NOT NULL,
    [_RecordDeleted]          INT            NOT NULL,
    [_RecordCurrent]          INT            NOT NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN);

