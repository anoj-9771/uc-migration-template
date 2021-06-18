CREATE TABLE [edw].[Prada_dbo_REPORT_DATE] (
    [REPORT_DATE]             NUMERIC (18) NULL,
    [REPORTING_YEAR]          NUMERIC (18) NULL,
    [QUARTER]                 NUMERIC (18) NULL,
    [MONTH_NUM]               NUMERIC (18) NULL,
    [MONTH_NAME]              VARCHAR (20) NULL,
    [WEEK]                    VARCHAR (3)  NULL,
    [EXTRACT_DATE]            NUMERIC (18) NULL,
    [WEEK_LAST_YEAR]          NUMERIC (18) NULL,
    [FY]                      VARCHAR (1)  NULL,
    [_DLRawZoneTimeStamp]     DATETIME     NULL,
    [_DLTrustedZoneTimeStamp] DATETIME     NULL
);

