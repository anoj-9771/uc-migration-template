CREATE TABLE [stage].[Dimbillingdocument] (
    [dimbillingDocumentSK]    BIGINT         NULL,
    [sourceSystemCode]        NVARCHAR (256) NULL,
    [billingDocumentNumber]   NVARCHAR (256) NULL,
    [billingPeriodStartDate]  DATE           NULL,
    [billingPeriodEndDate]    DATE           NULL,
    [billCreatedDate]         DATE           NULL,
    [isOutsortedFlag]         NVARCHAR (256) NULL,
    [isReversedFlag]          NVARCHAR (256) NULL,
    [reversalDate]            DATE           NULL,
    [_DLCuratedZoneTimeStamp] DATETIME2 (7)  NULL,
    [_RecordStart]            DATETIME2 (7)  NULL,
    [_RecordEnd]              DATETIME2 (7)  NULL,
    [_RecordDeleted]          INT            NULL,
    [_RecordCurrent]          INT            NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN);

